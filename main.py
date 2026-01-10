"""
CryExc Example Backend - FastAPI + DuckDB
Simple reference implementation for Binance Futures.
"""
import asyncio
import json
import logging
import math
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware

from binance import BinanceClient, fetch_tickers_24hr, fetch_klines, fetch_exchange_info
from database import db
from news import news_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SYMBOLS = ["BTCUSDT"]

binance_client: Optional[BinanceClient] = None

TIME_RANGE_MS = {
    "15m": 15 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "12h": 12 * 60 * 60 * 1000,
    "24h": 24 * 60 * 60 * 1000,
}

INTERVAL_MS = {
    "raw": 1000,
    "30s": 30 * 1000,
    "1m": 60 * 1000,
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
}


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[WebSocket, dict] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[websocket] = {"subscriptions": {}}

    def disconnect(self, websocket: WebSocket):
        self.active_connections.pop(websocket, None)

    def get_subscriptions(self, websocket: WebSocket) -> dict:
        conn = self.active_connections.get(websocket)
        return conn["subscriptions"] if conn else {}

    def subscribe(self, websocket: WebSocket, stream: str, config: dict,
                  instance_id: Optional[str] = None):
        subs = self.get_subscriptions(websocket)
        key = f"{stream}:{instance_id}" if instance_id else stream
        subs[key] = {"stream": stream, "config": config, "instanceId": instance_id}

    def unsubscribe(self, websocket: WebSocket, stream: str,
                    instance_id: Optional[str] = None):
        subs = self.get_subscriptions(websocket)
        key = f"{stream}:{instance_id}" if instance_id else stream
        subs.pop(key, None)

    def get_subscribers_for_stream(self, stream: str, symbol: str,
                                   exchange: str = "binancef") -> list[tuple[WebSocket, dict]]:
        result = []
        for ws, conn in self.active_connections.items():
            for key, sub in conn["subscriptions"].items():
                if sub["stream"] != stream:
                    continue
                config = sub["config"]
                cfg_symbol = config.get("symbol", "").upper()
                if cfg_symbol != symbol.upper():
                    continue
                cfg_exchanges = config.get("exchanges", [])
                cfg_exchange = config.get("exchange", "")
                if cfg_exchanges and exchange not in cfg_exchanges:
                    continue
                if cfg_exchange and cfg_exchange != exchange:
                    continue
                result.append((ws, sub))
        return result

    def get_all_subscribers_for_stream(self, stream: str) -> list[tuple[WebSocket, dict]]:
        result = []
        for ws, conn in self.active_connections.items():
            for key, sub in conn["subscriptions"].items():
                if sub["stream"] == stream:
                    result.append((ws, sub))
        return result


manager = ConnectionManager()


def compute_orderbook_stats(orderbook: dict) -> dict:
    """Compute orderbook statistics from raw orderbook."""
    bids = orderbook.get("bids", [])
    asks = orderbook.get("asks", [])

    if not bids or not asks:
        return None

    best_bid = bids[0]["price"] if bids else 0
    best_ask = asks[0]["price"] if asks else 0
    mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else 0
    spread = best_ask - best_bid if best_bid and best_ask else 0

    def sum_qty_within_pct(levels: list, ref_price: float, pct: float, is_bid: bool) -> float:
        total = 0
        for lvl in levels:
            price = lvl["price"]
            if is_bid:
                if price >= ref_price * (1 - pct / 100):
                    total += lvl["quantity"]
            else:
                if price <= ref_price * (1 + pct / 100):
                    total += lvl["quantity"]
        return total

    return {
        "exchange": orderbook["exchange"],
        "symbol": orderbook["symbol"],
        "timestamp": orderbook["timestamp"],
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "midPrice": mid_price,
        "spread": spread,
        "bidQuantity_0_5pct": sum_qty_within_pct(bids, mid_price, 0.5, True),
        "askQuantity_0_5pct": sum_qty_within_pct(asks, mid_price, 0.5, False),
        "bidQuantity_2pct": sum_qty_within_pct(bids, mid_price, 2, True),
        "askQuantity_2pct": sum_qty_within_pct(asks, mid_price, 2, False),
        "bidQuantity_10pct": sum_qty_within_pct(bids, mid_price, 10, True),
        "askQuantity_10pct": sum_qty_within_pct(asks, mid_price, 10, False),
        "totalBidQuantity": sum(lvl["quantity"] for lvl in bids),
        "totalAskQuantity": sum(lvl["quantity"] for lvl in asks),
    }


def group_orderbook_by_tick(orderbook: dict, tick_size: float) -> dict:
    """Group orderbook levels by tick size."""
    if tick_size <= 0:
        return orderbook

    bids_grouped: dict[float, float] = {}
    asks_grouped: dict[float, float] = {}

    for lvl in orderbook.get("bids", []):
        price_bucket = math.floor(lvl["price"] / tick_size) * tick_size
        bids_grouped[price_bucket] = bids_grouped.get(price_bucket, 0) + lvl["quantity"]

    for lvl in orderbook.get("asks", []):
        price_bucket = math.floor(lvl["price"] / tick_size) * tick_size
        asks_grouped[price_bucket] = asks_grouped.get(price_bucket, 0) + lvl["quantity"]

    return {
        "exchange": orderbook["exchange"],
        "symbol": orderbook["symbol"],
        "timestamp": orderbook["timestamp"],
        "bids": [{"price": p, "quantity": q} for p, q in sorted(bids_grouped.items(), reverse=True)],
        "asks": [{"price": p, "quantity": q} for p, q in sorted(asks_grouped.items())],
    }


async def broadcast_orderbook_stats():
    """Periodically broadcast orderbook stats to subscribers."""
    while True:
        await asyncio.sleep(0.5)
        if not binance_client:
            continue

        for symbol in SYMBOLS:
            subscribers = manager.get_subscribers_for_stream("orderbook_stats", symbol)
            if not subscribers:
                continue

            ob = binance_client.get_orderbook(symbol)
            if not ob:
                continue

            stats = compute_orderbook_stats(ob)
            if not stats:
                continue

            for ws, sub in subscribers:
                try:
                    await ws.send_json({"type": "orderbook_stats", "data": stats})
                except Exception:
                    pass


async def broadcast_dom():
    """Periodically broadcast DOM data to subscribers."""
    while True:
        await asyncio.sleep(0.5)
        if not binance_client:
            continue

        for ws, sub in manager.get_all_subscribers_for_stream("dom"):
            config = sub["config"]
            symbol = config.get("symbol", "").upper()
            exchange = config.get("exchange", "binancef")
            tick_size = config.get("tickSize", 10)
            time_range = config.get("timeRange", "1h")
            time_range_ms = TIME_RANGE_MS.get(time_range, 60 * 60 * 1000)

            ob = binance_client.get_orderbook(symbol)
            if not ob:
                continue

            trade_levels, session_high, session_low, data_start = db.get_dom_levels(
                exchange, symbol, tick_size, time_range_ms
            )

            ob_grouped = group_orderbook_by_tick(ob, tick_size)
            ob_bids = {lvl["price"]: lvl["quantity"] for lvl in ob_grouped["bids"]}
            ob_asks = {lvl["price"]: lvl["quantity"] for lvl in ob_grouped["asks"]}

            # Build a merged levels dict - start with trade levels
            levels_map: dict[float, dict] = {}
            for lvl in trade_levels:
                levels_map[lvl["price"]] = lvl

            # Add orderbook bid levels (creating new levels if needed)
            for price, qty in ob_bids.items():
                if price in levels_map:
                    levels_map[price]["bid"] = qty
                else:
                    levels_map[price] = {
                        "price": price,
                        "bid": qty,
                        "ask": 0,
                        "sold": 0,
                        "bought": 0,
                        "delta": 0,
                        "volume": 0,
                    }

            # Add orderbook ask levels (creating new levels if needed)
            for price, qty in ob_asks.items():
                if price in levels_map:
                    levels_map[price]["ask"] = qty
                else:
                    levels_map[price] = {
                        "price": price,
                        "bid": 0,
                        "ask": qty,
                        "sold": 0,
                        "bought": 0,
                        "delta": 0,
                        "volume": 0,
                    }

            # Sort by price descending
            levels = sorted(levels_map.values(), key=lambda x: -x["price"])

            mid_price = (ob["bids"][0]["price"] + ob["asks"][0]["price"]) / 2 if ob["bids"] and ob["asks"] else 0

            dom_data = {
                "exchange": exchange,
                "symbol": symbol,
                "midPrice": mid_price,
                "timestamp": int(time.time() * 1000),
                "dataStartTime": data_start,
                "sessionHigh": session_high,
                "sessionLow": session_low,
                "levels": levels,
            }

            try:
                await ws.send_json({"type": "dom", "data": dom_data})
            except Exception:
                pass


async def broadcast_footprint():
    """Periodically broadcast live footprint candle updates."""
    while True:
        await asyncio.sleep(0.5)  # 500ms for reasonable update rate

        for ws, sub in manager.get_all_subscribers_for_stream("footprint"):
            config = sub["config"]
            instance_id = sub.get("instanceId")
            symbol = config.get("symbol", "").upper()
            exchange = config.get("exchange", "binancef")
            interval = config.get("interval", "1m")
            tick_size = config.get("tickSize", 10)

            interval_ms = INTERVAL_MS.get(interval, 60 * 1000)
            now = int(time.time() * 1000)
            current_candle_start = (now // interval_ms) * interval_ms

            # Only query for the current candle (last 2 intervals to ensure we catch it)
            candles = db.get_footprint_candles(
                exchange, symbol, interval_ms, tick_size, interval_ms * 2
            )

            if not candles:
                continue

            # Find the current candle by timestamp
            current_candle = None
            for c in candles:
                if c["timestamp"] == current_candle_start:
                    current_candle = c
                    break

            if not current_candle:
                continue

            # Frontend expects: { instanceId: "...", candle: {...} } inside data
            data = {"candle": current_candle}
            if instance_id:
                data["instanceId"] = instance_id

            msg = {"type": "footprint", "data": data}
            if instance_id:
                msg["instanceId"] = instance_id

            try:
                await ws.send_json(msg)
            except Exception:
                pass


async def store_orderbook_snapshots():
    """Periodically store orderbook snapshots for heatmap at 1-minute boundaries."""
    last_minute_boundary: dict[str, int] = {}
    tick_size = 10  # Default tick size for storage

    while True:
        await asyncio.sleep(0.5)  # Check frequently but only store at minute boundaries
        if not binance_client:
            continue

        now = int(time.time() * 1000)
        one_minute_ms = 60 * 1000
        current_minute = (now // one_minute_ms) * one_minute_ms

        # Store snapshots for all tracked symbols at minute boundaries
        for symbol in SYMBOLS:
            exchange = "binancef"
            cache_key = f"{symbol}:{exchange}:{tick_size}"
            last_boundary = last_minute_boundary.get(cache_key, 0)

            # Only store once per minute boundary
            if current_minute <= last_boundary:
                continue

            ob = binance_client.get_orderbook(symbol)
            if not ob:
                continue

            ob_grouped = group_orderbook_by_tick(ob, tick_size)

            levels = []
            for lvl in ob_grouped["bids"]:
                levels.append({"price": lvl["price"], "bid_qty": lvl["quantity"], "ask_qty": 0})
            for lvl in ob_grouped["asks"]:
                existing = next((l for l in levels if l["price"] == lvl["price"]), None)
                if existing:
                    existing["ask_qty"] = lvl["quantity"]
                else:
                    levels.append({"price": lvl["price"], "bid_qty": 0, "ask_qty": lvl["quantity"]})

            # Use the minute boundary as timestamp (aligned with candles)
            db.insert_orderbook_snapshot(exchange, symbol, current_minute, tick_size, levels)
            last_minute_boundary[cache_key] = current_minute

            # Broadcast live update to subscribers
            heatmap_subs = manager.get_all_subscribers_for_stream("orderbook_heatmap")
            for ws, sub in heatmap_subs:
                sub_config = sub["config"]
                if sub_config.get("symbol", "").upper() != symbol:
                    continue
                if sub_config.get("exchange", "binancef") != exchange:
                    continue

                # Sort bids descending, asks ascending (best price first)
                bids = sorted(
                    [{"price": l["price"], "quantity": l["bid_qty"]} for l in levels if l["bid_qty"] > 0],
                    key=lambda x: -x["price"]
                )
                asks = sorted(
                    [{"price": l["price"], "quantity": l["ask_qty"]} for l in levels if l["ask_qty"] > 0],
                    key=lambda x: x["price"]
                )
                live_snapshot = {
                    "timestamp": current_minute,
                    "bids": bids,
                    "asks": asks,
                }

                msg = {
                    "type": "orderbook_heatmap",
                    "data": {
                        "exchange": exchange,
                        "symbol": symbol,
                        "live": live_snapshot
                    }
                }
                if sub.get("instanceId"):
                    msg["instanceId"] = sub["instanceId"]

                try:
                    await ws.send_json(msg)
                except Exception:
                    pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    global binance_client
    binance_client = BinanceClient(SYMBOLS)

    async def on_trade(trade: dict):
        db.insert_trade(
            trade["exchange"], trade["symbol"], trade["price"],
            trade["quantity"], trade["quoteQty"], trade["isBuyerMaker"],
            trade["timestamp"]
        )

        for ws, sub in manager.get_subscribers_for_stream("trade", trade["symbol"]):
            config = sub["config"]
            min_notional = config.get("minNotional", 0)
            if trade["quoteQty"] < min_notional:
                continue
            side_filter = config.get("side")
            if side_filter:
                is_buy = not trade["isBuyerMaker"]
                if side_filter == "buy" and not is_buy:
                    continue
                if side_filter == "sell" and is_buy:
                    continue
            try:
                await ws.send_json({"type": "trade", "data": trade})
            except Exception:
                pass

    async def on_depth(orderbook: dict):
        for ws, sub in manager.get_subscribers_for_stream("orderbook", orderbook["symbol"]):
            config = sub["config"]
            tick_size = config.get("tickSize", 0)
            depth = config.get("depth", 0)

            if tick_size > 0:
                ob = group_orderbook_by_tick(orderbook, tick_size)
            else:
                ob = orderbook.copy()
                ob["bids"] = list(orderbook["bids"])
                ob["asks"] = list(orderbook["asks"])

            if depth > 0:
                ob["bids"] = ob["bids"][:depth]
                ob["asks"] = ob["asks"][:depth]

            try:
                await ws.send_json({"type": "orderbook", "data": ob})
            except Exception:
                pass

    async def on_liquidation(liq: dict):
        for ws, sub in manager.get_subscribers_for_stream("liquidation", liq["symbol"]):
            config = sub["config"]
            min_notional = config.get("minNotional", 0)
            if liq["quoteQty"] < min_notional:
                continue
            try:
                await ws.send_json({"type": "liquidation", "data": liq})
            except Exception:
                pass

    binance_client.on_trade = on_trade
    binance_client.on_depth = on_depth
    binance_client.on_liquidation = on_liquidation

    async def on_news(news: dict):
        for ws, sub in manager.get_all_subscribers_for_stream("news"):
            try:
                await ws.send_json({"type": "news", "data": news})
            except Exception:
                pass

    news_client.on_news = on_news

    await binance_client.start()
    logger.info("Binance client started")

    await news_client.start()
    logger.info("News client started")

    asyncio.create_task(cleanup_task())
    asyncio.create_task(broadcast_orderbook_stats())
    asyncio.create_task(broadcast_dom())
    asyncio.create_task(broadcast_footprint())
    asyncio.create_task(store_orderbook_snapshots())

    yield

    await binance_client.stop()
    await news_client.stop()
    db.close()


async def cleanup_task():
    while True:
        await asyncio.sleep(3600)
        db.cleanup_old_trades()
        logger.info("Cleaned up old data")


app = FastAPI(title="CryExc Example Backend", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            msg_type = msg.get("type")

            if msg_type == "ping":
                await websocket.send_json({"type": "pong"})

            elif msg_type == "stream_subscribe":
                await handle_subscribe(websocket, msg)

            elif msg_type == "stream_subscribe_batch":
                for sub in msg.get("subscriptions", []):
                    await handle_subscribe(websocket, {
                        "type": "stream_subscribe",
                        "stream": sub.get("stream"),
                        "instanceId": sub.get("instanceId"),
                        "config": sub.get("config", {}),
                    })

            elif msg_type == "stream_update":
                await handle_update(websocket, msg)

            elif msg_type == "stream_unsubscribe":
                await handle_unsubscribe(websocket, msg)

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)


async def handle_subscribe(websocket: WebSocket, msg: dict):
    stream = msg.get("stream")
    config = msg.get("config", {})
    instance_id = msg.get("instanceId")

    manager.subscribe(websocket, stream, config, instance_id)

    response = {"type": "subscribed", "stream": stream, "config": config}
    if instance_id:
        response["instanceId"] = instance_id
    await websocket.send_json(response)

    if stream == "cvd":
        await send_cvd_historical(websocket, config, instance_id)
    elif stream == "footprint":
        await send_footprint_historical(websocket, config, instance_id)
    elif stream == "orderbook_heatmap":
        await send_orderbook_heatmap_historical(websocket, config, instance_id)
    elif stream == "news":
        await send_news_historical(websocket)

    logger.info(f"Subscribed to {stream}: {config}")


async def handle_update(websocket: WebSocket, msg: dict):
    stream = msg.get("stream")
    config = msg.get("config", {})
    instance_id = msg.get("instanceId")

    manager.subscribe(websocket, stream, config, instance_id)

    if stream == "cvd":
        await send_cvd_historical(websocket, config, instance_id)
    elif stream == "footprint":
        await send_footprint_historical(websocket, config, instance_id)
    elif stream == "orderbook_heatmap":
        await send_orderbook_heatmap_historical(websocket, config, instance_id)


async def handle_unsubscribe(websocket: WebSocket, msg: dict):
    stream = msg.get("stream")
    instance_id = msg.get("instanceId")

    manager.unsubscribe(websocket, stream, instance_id)

    response = {"type": "unsubscribed", "stream": stream}
    if instance_id:
        response["instanceId"] = instance_id
    await websocket.send_json(response)


async def send_cvd_historical(websocket: WebSocket, config: dict, instance_id: Optional[str]):
    symbol = config.get("symbol", "BTCUSDT").upper()
    exchanges = config.get("exchanges", [])
    exchange = exchanges[0] if exchanges else "binancef"
    time_range = config.get("timeRange", "1h")
    interval = config.get("interval", "1m")

    time_range_ms = TIME_RANGE_MS.get(time_range, 60 * 60 * 1000)
    interval_ms = INTERVAL_MS.get(interval, 60 * 1000)

    points = db.get_cvd_historical(exchange, symbol, interval_ms, time_range_ms)

    # Frontend expects flat array with exchange in each point
    data = [{"exchange": exchange, **pt} for pt in points]

    msg = {"type": "cvd_historical", "data": data}
    if instance_id:
        msg["instanceId"] = instance_id

    await websocket.send_json(msg)


async def send_footprint_historical(websocket: WebSocket, config: dict, instance_id: Optional[str]):
    symbol = config.get("symbol", "BTCUSDT").upper()
    exchange = config.get("exchange", "binancef")
    interval = config.get("interval", "1m")
    tick_size = config.get("tickSize", 10)
    time_range = config.get("timeRange", "1h")

    time_range_ms = TIME_RANGE_MS.get(time_range, 60 * 60 * 1000)
    interval_ms = INTERVAL_MS.get(interval, 60 * 1000)

    candles = db.get_footprint_candles(exchange, symbol, interval_ms, tick_size, time_range_ms)

    # Frontend expects: { instanceId: "...", candles: [...] } inside data
    data = {"candles": candles}
    if instance_id:
        data["instanceId"] = instance_id

    msg = {"type": "footprint_historical", "data": data}
    if instance_id:
        msg["instanceId"] = instance_id

    await websocket.send_json(msg)


async def send_orderbook_heatmap_historical(websocket: WebSocket, config: dict, instance_id: Optional[str]):
    symbol = config.get("symbol", "BTCUSDT").upper()
    exchange = config.get("exchange", "binancef")
    interval = config.get("interval", "1m")
    tick_size = config.get("tickSize", 10)
    time_range = config.get("timeRange", "1h")

    time_range_ms = TIME_RANGE_MS.get(time_range, 60 * 60 * 1000)
    interval_ms = INTERVAL_MS.get(interval, 60 * 1000)

    snapshots = db.get_orderbook_heatmap_historical(exchange, symbol, tick_size, time_range_ms, interval_ms)

    logger.info(f"Sending {len(snapshots)} historical heatmap snapshots for {symbol}")
    if snapshots:
        first = snapshots[0]
        logger.info(f"First snapshot: ts={first['timestamp']}, bids={len(first['bids'])}, asks={len(first['asks'])}")
        if first['bids']:
            logger.info(f"First bid: {first['bids'][0]}")
        if first['asks']:
            logger.info(f"First ask: {first['asks'][0]}")

    # Frontend expects: { exchange, symbol, historical: [...] } format
    msg = {
        "type": "orderbook_heatmap",
        "data": {
            "exchange": exchange,
            "symbol": symbol,
            "historical": snapshots
        }
    }
    if instance_id:
        msg["instanceId"] = instance_id

    await websocket.send_json(msg)


async def send_news_historical(websocket: WebSocket):
    history = news_client.get_history()

    logger.info(f"Sending {len(history)} historical news items")

    msg = {"type": "news_historical", "data": history}
    await websocket.send_json(msg)


@app.get("/api/exchanges")
async def get_exchanges():
    try:
        symbols = await fetch_exchange_info()
    except Exception:
        symbols = SYMBOLS

    return [{
        "name": "binancef",
        "symbols": symbols,
        "capabilities": {
            "trades": True,
            "orderbook": True,
            "liquidations": True,
            "marketStats": True,
            "orderbookHeatmap": True,
            "screener": True,
        }
    }]


@app.get("/api/features")
async def get_features():
    return {
        "pythMarkets": False,
        "walletCohorts": False,
        "options": False,
        "news": True,
    }


_market_stats_cache: dict = {"data": [], "timestamp": 0}
_market_stats_cache_ttl = 10 * 60 * 1000  # 10 minutes in ms

@app.get("/api/market-stats")
async def get_market_stats():
    global _market_stats_cache
    now = int(time.time() * 1000)

    if now - _market_stats_cache["timestamp"] < _market_stats_cache_ttl:
        return _market_stats_cache["data"]

    if not binance_client:
        return []

    result = []
    for symbol in SYMBOLS:
        stats = binance_client.get_market_stats(symbol)
        if stats:
            result.append(stats)

    _market_stats_cache = {"data": result, "timestamp": now}
    return result


@app.get("/screener")
async def get_screener():
    try:
        return await fetch_tickers_24hr()
    except Exception as e:
        logger.error(f"Error fetching tickers: {e}")
        return []


@app.get("/klines")
async def get_klines(
    symbol: str = Query(...),
    interval: str = Query(default="1m"),
    limit: int = Query(default=500),
):
    try:
        return await fetch_klines(symbol, interval, limit)
    except Exception as e:
        logger.error(f"Error fetching klines: {e}")
        return []


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8086)
