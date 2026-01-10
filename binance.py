import asyncio
import json
import logging
import time
from typing import Callable, Optional
import websockets
import httpx

logger = logging.getLogger(__name__)


class BinanceClient:
    BASE_WS_URL = "wss://fstream.binance.com/stream"
    BASE_REST_URL = "https://fapi.binance.com"

    def __init__(self, symbols: list[str]):
        self.symbols = [s.upper() for s in symbols]
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.running = False
        self.on_trade: Optional[Callable] = None
        self.on_depth: Optional[Callable] = None
        self.on_liquidation: Optional[Callable] = None
        self.on_mark_price: Optional[Callable] = None

        self.orderbooks: dict[str, dict] = {}
        self.last_update_ids: dict[str, int] = {}
        self.initialized: dict[str, bool] = {}
        self.depth_buffers: dict[str, list] = {}

        self.mark_prices: dict[str, float] = {}
        self.index_prices: dict[str, float] = {}
        self.funding_rates: dict[str, float] = {}
        self.next_funding_times: dict[str, int] = {}
        self.open_interest: dict[str, float] = {}
        self.long_short_ratio: dict[str, float] = {}
        self.long_account: dict[str, float] = {}
        self.short_account: dict[str, float] = {}

    def _build_ws_url(self) -> str:
        streams = []
        for sym in self.symbols:
            sym_lower = sym.lower()
            streams.extend([
                f"{sym_lower}@trade",
                f"{sym_lower}@depth@100ms",
                f"{sym_lower}@markPrice",
                f"{sym_lower}@forceOrder",
            ])
        return f"{self.BASE_WS_URL}?streams={'/'.join(streams)}"

    async def start(self):
        self.running = True
        asyncio.create_task(self._run_ws())
        asyncio.create_task(self._poll_open_interest())

    async def stop(self):
        self.running = False
        if self.ws:
            await self.ws.close()

    async def _run_ws(self):
        while self.running:
            try:
                url = self._build_ws_url()
                logger.info(f"Connecting to Binance: {url[:100]}...")
                async with websockets.connect(url, ping_interval=30) as ws:
                    self.ws = ws
                    logger.info("Connected to Binance Futures")

                    for symbol in self.symbols:
                        self.initialized[symbol] = False
                        self.depth_buffers[symbol] = []
                        asyncio.create_task(self._fetch_snapshot(symbol))

                    async for msg in ws:
                        await self._handle_message(msg)
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                await asyncio.sleep(5)

    async def _handle_message(self, msg: str):
        try:
            data = json.loads(msg)
            stream = data.get("stream", "")

            if "@trade" in stream:
                await self._handle_trade(data["data"])
            elif "@depth" in stream:
                await self._handle_depth(data["data"])
            elif "@markPrice" in stream:
                self._handle_mark_price(data["data"])
            elif "@forceOrder" in stream:
                await self._handle_liquidation(data["data"])
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    async def _handle_trade(self, data: dict):
        if data.get("e") != "trade":
            return

        trade = {
            "exchange": "binancef",
            "symbol": data["s"],
            "price": float(data["p"]),
            "quantity": float(data["q"]),
            "quoteQty": float(data["p"]) * float(data["q"]),
            "isBuyerMaker": data["m"],
            "timestamp": data["T"],
        }

        if self.on_trade:
            await self.on_trade(trade)

    async def _handle_depth(self, data: dict):
        symbol = data["s"]
        update = {
            "first_update_id": data["U"],
            "final_update_id": data["u"],
            "prev_update_id": data.get("pu", 0),
            "bids": {float(b[0]): float(b[1]) for b in data["b"]},
            "asks": {float(a[0]): float(a[1]) for a in data["a"]},
            "event_time": data["E"],
        }

        if not self.initialized.get(symbol, False):
            self.depth_buffers.setdefault(symbol, []).append(update)
            return

        last_id = self.last_update_ids.get(symbol, 0)
        if last_id > 0 and update["prev_update_id"] != last_id:
            logger.warning(f"Sequence break for {symbol}, re-initializing")
            self.initialized[symbol] = False
            self.depth_buffers[symbol] = [update]
            asyncio.create_task(self._fetch_snapshot(symbol))
            return

        self.last_update_ids[symbol] = update["final_update_id"]
        self._apply_depth_update(symbol, update)
        await self._emit_orderbook(symbol)

    async def _fetch_snapshot(self, symbol: str):
        try:
            async with httpx.AsyncClient() as client:
                url = f"{self.BASE_REST_URL}/fapi/v1/depth?symbol={symbol}&limit=1000"
                resp = await client.get(url, timeout=10)
                resp.raise_for_status()
                data = resp.json()

            snapshot_id = data["lastUpdateId"]
            bids = {float(b[0]): float(b[1]) for b in data["bids"]}
            asks = {float(a[0]): float(a[1]) for a in data["asks"]}

            self.orderbooks[symbol] = {"bids": bids, "asks": asks}
            self.last_update_ids[symbol] = snapshot_id

            buffered = self.depth_buffers.get(symbol, [])
            self.depth_buffers[symbol] = []

            valid_updates = [
                u for u in buffered
                if u["final_update_id"] > snapshot_id
                and u["first_update_id"] <= snapshot_id + 1
            ]

            for update in sorted(valid_updates, key=lambda x: x["first_update_id"]):
                self._apply_depth_update(symbol, update)
                self.last_update_ids[symbol] = update["final_update_id"]

            self.initialized[symbol] = True
            logger.info(f"Orderbook initialized for {symbol}")
            await self._emit_orderbook(symbol)

        except Exception as e:
            logger.error(f"Error fetching snapshot for {symbol}: {e}")
            await asyncio.sleep(2)
            asyncio.create_task(self._fetch_snapshot(symbol))

    def _apply_depth_update(self, symbol: str, update: dict):
        ob = self.orderbooks.get(symbol)
        if not ob:
            return

        for price, qty in update["bids"].items():
            if qty == 0:
                ob["bids"].pop(price, None)
            else:
                ob["bids"][price] = qty

        for price, qty in update["asks"].items():
            if qty == 0:
                ob["asks"].pop(price, None)
            else:
                ob["asks"][price] = qty

    async def _emit_orderbook(self, symbol: str):
        ob = self.orderbooks.get(symbol)
        if not ob or not self.on_depth:
            return

        sorted_bids = sorted(ob["bids"].items(), key=lambda x: -x[0])
        sorted_asks = sorted(ob["asks"].items(), key=lambda x: x[0])

        best_bid = sorted_bids[0][0] if sorted_bids else 0
        best_ask = sorted_asks[0][0] if sorted_asks else 0
        mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else 0

        orderbook = {
            "exchange": "binancef",
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "bestBid": best_bid,
            "bestAsk": best_ask,
            "midPrice": mid_price,
            "bids": [{"price": p, "quantity": q} for p, q in sorted_bids],
            "asks": [{"price": p, "quantity": q} for p, q in sorted_asks],
        }

        await self.on_depth(orderbook)

    def _handle_mark_price(self, data: dict):
        if data.get("e") != "markPriceUpdate":
            return

        symbol = data["s"]
        self.mark_prices[symbol] = float(data["p"])
        self.index_prices[symbol] = float(data["i"])
        self.funding_rates[symbol] = float(data["r"])
        self.next_funding_times[symbol] = data["T"]

        if self.on_mark_price:
            asyncio.create_task(self.on_mark_price({
                "symbol": symbol,
                "markPrice": self.mark_prices[symbol],
                "indexPrice": self.index_prices[symbol],
                "fundingRate": self.funding_rates[symbol],
                "nextFundingTime": self.next_funding_times[symbol],
            }))

    async def _handle_liquidation(self, data: dict):
        if data.get("e") != "forceOrder":
            return

        order = data["o"]
        price = float(order["p"])
        qty = float(order["q"])

        liquidation = {
            "exchange": "binancef",
            "symbol": order["s"],
            "side": "SELL" if order["S"] == "SELL" else "BUY",
            "price": price,
            "quantity": qty,
            "notionalUsd": price * qty,
            "timestamp": data["E"],
        }

        if self.on_liquidation:
            await self.on_liquidation(liquidation)

    async def _poll_open_interest(self):
        while self.running:
            try:
                async with httpx.AsyncClient() as client:
                    for symbol in self.symbols:
                        # Poll open interest
                        url = f"{self.BASE_REST_URL}/fapi/v1/openInterest?symbol={symbol}"
                        resp = await client.get(url, timeout=10)
                        if resp.status_code == 200:
                            data = resp.json()
                            self.open_interest[symbol] = float(data["openInterest"])
                        await asyncio.sleep(0.1)

                        # Poll long/short ratio
                        url = f"{self.BASE_REST_URL}/futures/data/globalLongShortAccountRatio?symbol={symbol}&period=5m&limit=1"
                        resp = await client.get(url, timeout=10)
                        if resp.status_code == 200:
                            data = resp.json()
                            if data:
                                self.long_short_ratio[symbol] = float(data[0].get("longShortRatio", 0))
                                self.long_account[symbol] = float(data[0].get("longAccount", 0))
                                self.short_account[symbol] = float(data[0].get("shortAccount", 0))
                        await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error polling OI/LSR: {e}")
            await asyncio.sleep(300)  # Poll every 5 minutes - this data doesn't change fast

    def get_orderbook(self, symbol: str) -> Optional[dict]:
        ob = self.orderbooks.get(symbol.upper())
        if not ob:
            return None

        sorted_bids = sorted(ob["bids"].items(), key=lambda x: -x[0])
        sorted_asks = sorted(ob["asks"].items(), key=lambda x: x[0])

        best_bid = sorted_bids[0][0] if sorted_bids else 0
        best_ask = sorted_asks[0][0] if sorted_asks else 0
        mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else 0

        return {
            "exchange": "binancef",
            "symbol": symbol.upper(),
            "timestamp": int(time.time() * 1000),
            "bestBid": best_bid,
            "bestAsk": best_ask,
            "midPrice": mid_price,
            "bids": [{"price": p, "quantity": q} for p, q in sorted_bids],
            "asks": [{"price": p, "quantity": q} for p, q in sorted_asks],
        }

    def get_market_stats(self, symbol: str) -> Optional[dict]:
        symbol = symbol.upper()
        if symbol not in self.mark_prices:
            return None

        mark = self.mark_prices.get(symbol, 0)
        index = self.index_prices.get(symbol, 0)
        oi = self.open_interest.get(symbol, 0)
        oi_usd = oi * mark if mark > 0 else 0

        # Calculate basis (mark - index) and basis in bps
        basis = mark - index if index > 0 else 0
        basis_bps = (basis / index * 10000) if index > 0 else 0

        return {
            "exchange": "binancef",
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "fundingRate": self.funding_rates.get(symbol, 0),
            "nextFundingTime": self.next_funding_times.get(symbol, 0),
            "openInterest": oi,
            "openInterestUsd": oi_usd,
            "indexPrice": index,
            "markPrice": mark,
            "basis": basis,
            "basisBps": basis_bps,
            "longShortRatio": self.long_short_ratio.get(symbol, 0),
            "longAccount": self.long_account.get(symbol, 0),
            "shortAccount": self.short_account.get(symbol, 0),
        }


async def fetch_tickers_24hr() -> list[dict]:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://fapi.binance.com/fapi/v1/ticker/24hr",
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()

    now = int(time.time() * 1000)
    one_day_ago = now - 24 * 60 * 60 * 1000

    tickers = []
    for t in data:
        if t.get("closeTime", 0) < one_day_ago:
            continue
        tickers.append({
            "exchange": "binancef",
            "symbol": t["symbol"],
            "lastPrice": float(t["lastPrice"]),
            "priceChange": float(t["priceChange"]),
            "priceChangePercent": float(t["priceChangePercent"]),
            "volume24h": float(t["volume"]),
            "quoteVolume24h": float(t["quoteVolume"]),
            "high24h": float(t["highPrice"]),
            "low24h": float(t["lowPrice"]),
            "tradeCount": t["count"],
            "timestamp": now,
        })
    return tickers


async def fetch_klines(symbol: str, interval: str = "1m", limit: int = 500) -> list[dict]:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"https://fapi.binance.com/fapi/v1/klines",
            params={"symbol": symbol.upper(), "interval": interval, "limit": limit},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()

    return [
        {
            "timestamp": k[0],
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
        }
        for k in data
    ]


async def fetch_exchange_info() -> list[str]:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://fapi.binance.com/fapi/v1/exchangeInfo",
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()

    symbols = []
    for s in data.get("symbols", []):
        if s.get("status") == "TRADING" and s.get("contractType") == "PERPETUAL":
            symbols.append(s["symbol"])
    return sorted(symbols)
