import duckdb
import time
import math
from typing import Optional


class Database:
    def __init__(self, path: str = ":memory:"):
        self.conn = duckdb.connect(path)
        self._init_tables()

    def _init_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                exchange VARCHAR,
                symbol VARCHAR,
                price DOUBLE,
                qty DOUBLE,
                quote_qty DOUBLE,
                is_buyer_maker BOOLEAN,
                timestamp BIGINT
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                exchange VARCHAR,
                symbol VARCHAR,
                timestamp BIGINT,
                tick_size DOUBLE,
                price DOUBLE,
                bid_qty DOUBLE,
                ask_qty DOUBLE
            )
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_trades_lookup
            ON trades (exchange, symbol, timestamp)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ob_snapshots_lookup
            ON orderbook_snapshots (exchange, symbol, tick_size, timestamp)
        """)

    def insert_trade(self, exchange: str, symbol: str, price: float, qty: float,
                     quote_qty: float, is_buyer_maker: bool, timestamp: int):
        # Skip invalid trades with 0 price or quantity
        if price <= 0 or qty <= 0:
            return
        self.conn.execute("""
            INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [exchange, symbol, price, qty, quote_qty, is_buyer_maker, timestamp])

    def insert_trades_batch(self, trades: list[tuple]):
        if not trades:
            return
        # Filter out invalid trades with 0 price or quantity
        valid_trades = [t for t in trades if t[2] > 0 and t[3] > 0]  # price at index 2, qty at index 3
        if not valid_trades:
            return
        self.conn.executemany("""
            INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?)
        """, valid_trades)

    def insert_orderbook_snapshot(self, exchange: str, symbol: str, timestamp: int,
                                  tick_size: float, levels: list[dict]):
        """Store orderbook snapshot with price levels grouped by tick_size."""
        if not levels:
            return
        rows = [
            (exchange, symbol, timestamp, tick_size,
             lvl["price"], lvl.get("bid_qty", 0), lvl.get("ask_qty", 0))
            for lvl in levels
        ]
        self.conn.executemany("""
            INSERT INTO orderbook_snapshots VALUES (?, ?, ?, ?, ?, ?, ?)
        """, rows)

    def get_cvd_historical(self, exchange: str, symbol: str, interval_ms: int,
                           time_range_ms: int) -> list[dict]:
        """Get CVD data aggregated by interval."""
        now = int(time.time() * 1000)
        start_time = now - time_range_ms

        # Use // for integer floor division
        result = self.conn.execute("""
            SELECT
                (timestamp // ?) * ? as bucket_ts,
                SUM(CASE WHEN NOT is_buyer_maker THEN quote_qty ELSE 0 END) as buy_volume,
                SUM(CASE WHEN is_buyer_maker THEN quote_qty ELSE 0 END) as sell_volume
            FROM trades
            WHERE exchange = ? AND symbol = ? AND timestamp >= ?
            GROUP BY bucket_ts
            ORDER BY bucket_ts
        """, [interval_ms, interval_ms, exchange, symbol, start_time]).fetchall()

        cumulative = 0
        points = []
        for row in result:
            buy_vol = row[1] or 0
            sell_vol = row[2] or 0
            delta = buy_vol - sell_vol
            cumulative += delta
            points.append({
                "timestamp": row[0],
                "value": cumulative,
                "buyVolume": buy_vol,
                "sellVolume": sell_vol
            })
        return points

    def get_footprint_candles(self, exchange: str, symbol: str, interval_ms: int,
                              tick_size: float, time_range_ms: int) -> list[dict]:
        """Build footprint candles from trades."""
        now = int(time.time() * 1000)
        start_time = now - time_range_ms

        # Use // for integer floor division to properly bucket timestamps
        result = self.conn.execute("""
            SELECT
                (timestamp // ?) * ? as candle_time,
                FLOOR(price / ?) * ? as price_level,
                MIN(price) as low,
                MAX(price) as high,
                SUM(CASE WHEN is_buyer_maker THEN qty ELSE 0 END) as bid_volume,
                SUM(CASE WHEN NOT is_buyer_maker THEN qty ELSE 0 END) as ask_volume,
                SUM(qty) as total_volume
            FROM trades
            WHERE exchange = ? AND symbol = ? AND timestamp >= ?
            GROUP BY candle_time, price_level
            ORDER BY candle_time, price_level
        """, [interval_ms, interval_ms, tick_size, tick_size,
              exchange, symbol, start_time]).fetchall()

        candles_map: dict[int, dict] = {}

        for row in result:
            candle_time = int(row[0])
            price_level = row[1]
            low = row[2]
            high = row[3]
            bid_vol = row[4] or 0
            ask_vol = row[5] or 0
            total_vol = row[6] or 0

            if candle_time not in candles_map:
                candles_map[candle_time] = {
                    "timestamp": candle_time,
                    "open": price_level,
                    "high": high,
                    "low": low,
                    "close": price_level,
                    "volume": 0,
                    "buyVolume": 0,
                    "sellVolume": 0,
                    "footprint": {}
                }

            candle = candles_map[candle_time]
            candle["high"] = max(candle["high"], high)
            candle["low"] = min(candle["low"], low)
            candle["close"] = price_level
            candle["volume"] += total_vol
            candle["buyVolume"] += ask_vol
            candle["sellVolume"] += bid_vol
            # Frontend expects footprint as object with price as string key
            # Format price key with 1 decimal place like Go backend does
            price_key = f"{price_level:.1f}"
            candle["footprint"][price_key] = {
                "bidVolume": bid_vol,
                "askVolume": ask_vol,
                "totalVolume": total_vol
            }

        # Get proper open/close using arg_min/arg_max to find price at min/max timestamp
        ohlc_result = self.conn.execute("""
            SELECT
                (timestamp // ?) * ? as candle_time,
                arg_min(price, timestamp) as open_price,
                arg_max(price, timestamp) as close_price
            FROM trades
            WHERE exchange = ? AND symbol = ? AND timestamp >= ?
            GROUP BY candle_time
        """, [interval_ms, interval_ms, exchange, symbol, start_time]).fetchall()

        for row in ohlc_result:
            candle_time = int(row[0])
            if candle_time in candles_map:
                candles_map[candle_time]["open"] = row[1]
                candles_map[candle_time]["close"] = row[2]

        return [candles_map[k] for k in sorted(candles_map.keys())]

    def get_dom_levels(self, exchange: str, symbol: str, tick_size: float,
                       time_range_ms: int) -> tuple[list[dict], float, float, int]:
        """Get DOM data: trades aggregated by price level."""
        now = int(time.time() * 1000)
        start_time = now - time_range_ms

        result = self.conn.execute("""
            SELECT
                FLOOR(price / ?) * ? as price_level,
                SUM(CASE WHEN is_buyer_maker THEN qty ELSE 0 END) as sold,
                SUM(CASE WHEN NOT is_buyer_maker THEN qty ELSE 0 END) as bought,
                SUM(qty) as volume,
                MAX(price) as session_high,
                MIN(price) as session_low
            FROM trades
            WHERE exchange = ? AND symbol = ? AND timestamp >= ?
            GROUP BY price_level
            ORDER BY price_level DESC
        """, [tick_size, tick_size, exchange, symbol, start_time]).fetchall()

        if not result:
            return [], 0, 0, start_time

        session_high = max(row[4] for row in result)
        session_low = min(row[5] for row in result)

        levels = []
        for row in result:
            price = row[0]
            sold = row[1] or 0
            bought = row[2] or 0
            volume = row[3] or 0
            levels.append({
                "price": price,
                "bid": 0,
                "ask": 0,
                "sold": sold,
                "bought": bought,
                "delta": bought - sold,
                "volume": volume
            })

        return levels, session_high, session_low, start_time

    def get_orderbook_heatmap_historical(self, exchange: str, symbol: str,
                                         tick_size: float, time_range_ms: int,
                                         interval_ms: int) -> list[dict]:
        """Get historical orderbook snapshots for heatmap."""
        now = int(time.time() * 1000)
        start_time = now - time_range_ms

        # Don't filter by tick_size - return all stored snapshots for this symbol
        result = self.conn.execute("""
            SELECT timestamp, price, bid_qty, ask_qty
            FROM orderbook_snapshots
            WHERE exchange = ? AND symbol = ? AND timestamp >= ?
            ORDER BY timestamp, price
        """, [exchange, symbol, start_time]).fetchall()

        snapshots_map: dict[int, dict] = {}
        for row in result:
            ts = row[0]
            if ts not in snapshots_map:
                snapshots_map[ts] = {"timestamp": ts, "bids": [], "asks": []}

            if row[2] > 0:
                snapshots_map[ts]["bids"].append({"price": row[1], "quantity": row[2]})
            if row[3] > 0:
                snapshots_map[ts]["asks"].append({"price": row[1], "quantity": row[3]})

        # Sort bids descending (highest first) and asks ascending (lowest first)
        # so first element is best bid/ask for midPrice calculation
        snapshots = []
        for ts in sorted(snapshots_map.keys()):
            snap = snapshots_map[ts]
            snap["bids"] = sorted(snap["bids"], key=lambda x: -x["price"])
            snap["asks"] = sorted(snap["asks"], key=lambda x: x["price"])
            snapshots.append(snap)

        return snapshots

    def cleanup_old_trades(self, max_age_ms: int = 24 * 60 * 60 * 1000):
        """Remove trades older than max_age_ms."""
        cutoff = int(time.time() * 1000) - max_age_ms
        self.conn.execute("DELETE FROM trades WHERE timestamp < ?", [cutoff])
        self.conn.execute("DELETE FROM orderbook_snapshots WHERE timestamp < ?", [cutoff])

    def close(self):
        self.conn.close()


db = Database("cryexc.duckdb")
