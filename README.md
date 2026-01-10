# CryExc Example Backend

A simple FastAPI + DuckDB reference implementation for CryExc frontend.

Currently supports **Binance Futures** only. This is meant as a starting point for users who want to build their own backend.

> **Note:** If you're in a region where Binance is blocked (US, etc.), use exchanges that operate in your region like [Hyperliquid](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/).

## Quick Start

```bash
# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt

# Run the server
python main.py
```

Server runs on `http://localhost:8086` by default.

## Connecting to CryExc

1. Start this backend: `python main.py`
2. Open [cryexc.josedonato.com](https://cryexc.josedonato.com)
3. Launch the app
4. Click connection settings
5. Select "Self-Hosted Backend"
6. Enter `localhost:8086`
7. Click Connect

## Configuration
- Port - Change in the `uvicorn.run()` call at the bottom

## Implemented Features

### WebSocket Streams (`/ws`)

| Stream | Status | Description |
|--------|--------|-------------|
| `trade` | Implemented | Real-time trades with minNotional and side filters |
| `orderbook` | Implemented | Order book depth with tickSize grouping |
| `liquidation` | Implemented | Liquidation events with minNotional filter |
| `cvd` | Implemented | Historical + live CVD from stored trades |
| `orderbook_stats` | Implemented | Aggregated stats (spread, depth at %) |
| `dom` | Implemented | Depth of Market with trade history by price |
| `footprint` | Implemented | Instance-based footprint candles |
| `orderbook_heatmap` | Implemented | Instance-based orderbook snapshots |
| `news` | Implemented | Real-time news from Tree of Alpha |

### HTTP Endpoints

| Endpoint | Status | Description |
|----------|--------|-------------|
| `GET /api/exchanges` | Implemented | Available exchanges and symbols |
| `GET /api/features` | Implemented | Feature flags (all disabled) |
| `GET /api/market-stats` | Implemented | Funding rate, OI, mark/index prices |
| `GET /screener` | Implemented | 24h ticker data |
| `GET /klines` | Implemented | OHLCV candles |


## Architecture

```
main.py           - FastAPI app, WebSocket handling, HTTP endpoints
binance.py        - Binance Futures WebSocket client
database.py       - DuckDB for trade storage and CVD aggregation
news.py           - Tree of Alpha news WebSocket client
models.py         - Pydantic models
```

## How It Works

1. **Binance Client** connects to Binance Futures WebSocket and subscribes to:
   - `@trade` - Real-time trades
   - `@depth@100ms` - Orderbook updates
   - `@markPrice` - Funding rate and mark/index prices
   - `@forceOrder` - Liquidations

2. **DuckDB** stores all trades in memory for CVD calculation. Trades older than 24h are cleaned up hourly.

3. **WebSocket Server** manages client subscriptions and broadcasts data based on each client's filters (symbol, minNotional, exchanges, side).

## Stream Details

### Footprint

Built from trades stored in DuckDB, aggregated by candle interval and tick size. On subscribe, historical candles are sent. Live updates broadcast the current candle every second.

### Orderbook Heatmap

Orderbook snapshots are stored in DuckDB at the configured interval. On subscribe, historical snapshots are sent. Live snapshots are broadcast as they're captured.

### DOM (Depth of Market)

Combines:
- Current orderbook (bid/ask at each price level)
- Historical trade volume by price level (bought/sold/delta)

Broadcasts every 500ms to subscribers.

### Orderbook Stats

Computed from current orderbook every 500ms:
- Best bid/ask, mid price, spread
- Total quantity within 0.5%, 2%, 10% of mid price
- Total bid/ask quantity

### News

Connects to Tree of Alpha WebSocket (`wss://news.treeofalpha.com/ws`) for real-time crypto news. On subscribe, historical news items are sent. Live news broadcasts as they arrive.

## Extending

### Adding a new exchange

1. Create a new client class similar to `BinanceClient` in `binance.py`
2. Implement the same callbacks: `on_trade`, `on_depth`, `on_liquidation`
3. Start it in the FastAPI lifespan handler

## API Specification

See `../website/static/llms.txt` for the complete API specification including all message formats, field definitions, and implementation details. Use this file as reference when building your own backend.

## Important Implementation Details

These are critical details we discovered during implementation that aren't obvious from the frontend code:

### Timestamp Alignment

All candle-based data (footprint, CVD, heatmap) must use floor division for timestamp bucketing:
```python
candle_time = (timestamp // interval_ms) * interval_ms
```

### Trade Direction

The `isBuyerMaker` field determines trade direction:
- `true` = **sell** (taker sold to maker's bid)
- `false` = **buy** (taker bought from maker's ask)

This affects CVD calculation and footprint `bidVolume`/`askVolume`.

### Footprint Price Keys

Footprint data uses price levels as **string keys with 1 decimal place**:
```python
price_key = f"{price_level:.1f}"  # "42000.0", not "42000" or 42000
```

### Market Stats Fields

The Symbol Summary widget requires these calculated fields:
- `openInterestUsd` = `openInterest * markPrice` (displayed as "$X")
- `longShortRatio`, `longAccount`, `shortAccount` from exchange's global L/S API
- `basis` = `markPrice - indexPrice`
- `basisBps` = `(basis / indexPrice) * 10000`

### DOM Levels

DOM must merge orderbook prices with trade prices. Create levels for orderbook prices even if no trades occurred there:
```python
# For each orderbook price not in trade data:
{"price": X, "bid": qty, "ask": 0, "sold": 0, "bought": 0, "delta": 0, "volume": 0}
```

### Orderbook Heatmap

- Store snapshots at **candle boundary timestamps** (same as footprint)
- Sort `bids` **descending** by price (best bid first)
- Sort `asks` **ascending** by price (best ask first)
- Include `exchange` and `symbol` in the message data object

### Orderbook Depth

When `depth=0` in subscription config, return **all** orderbook levels, not a limited subset.

### Caching

Cache expensive data aggressively:
- `/api/market-stats`: 10 minutes
- OI/Long-Short Ratio polling: 5 minutes (data doesn't change fast)

### Invalid Trade Protection

Filter out trades with `price <= 0` or `quantity <= 0` before storing.

## Notes

- DuckDB stores data to `cryexc.duckdb` file for persistence across restarts
- Binance has rate limits - be careful when fetching historical data
- Orderbook maintains full state locally and emits snapshots (not deltas)
- Data older than 24h is cleaned up hourly
- Orderbook heatmap snapshots are stored at 1-minute boundaries aligned with candle times
- News client keeps last 100 items in memory for historical data on subscribe
