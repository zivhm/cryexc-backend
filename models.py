from pydantic import BaseModel
from typing import Optional


class TradeConfig(BaseModel):
    symbol: str
    exchanges: list[str] = []
    minNotional: float = 0
    side: Optional[str] = None


class OrderbookConfig(BaseModel):
    symbol: str
    exchanges: list[str] = []
    tickSize: float = 0
    depth: int = 20


class LiquidationConfig(BaseModel):
    symbol: str
    exchanges: list[str] = []
    minNotional: float = 0


class CVDConfig(BaseModel):
    symbol: str
    exchanges: list[str] = []
    interval: str = "raw"
    timeRange: str = "1h"


class DOMConfig(BaseModel):
    symbol: str
    exchange: str
    tickSize: float
    timeRange: str = "1h"


class FootprintConfig(BaseModel):
    symbol: str
    exchange: str
    interval: str
    tickSize: float
    timeRange: str


class OrderbookHeatmapConfig(BaseModel):
    symbol: str
    exchange: str
    interval: str
    tickSize: float
    timeRange: str


class OrderbookStatsConfig(BaseModel):
    symbol: str
    exchanges: list[str] = []


class StreamSubscribe(BaseModel):
    type: str = "stream_subscribe"
    stream: str
    instanceId: Optional[str] = None
    config: dict


class StreamUpdate(BaseModel):
    type: str = "stream_update"
    stream: str
    instanceId: Optional[str] = None
    config: dict


class StreamUnsubscribe(BaseModel):
    type: str = "stream_unsubscribe"
    stream: str
    instanceId: Optional[str] = None


class StreamSubscribeBatch(BaseModel):
    type: str = "stream_subscribe_batch"
    subscriptions: list[dict]


class Trade(BaseModel):
    exchange: str
    symbol: str
    price: float
    qty: float
    quoteQty: float
    isBuyerMaker: bool
    timestamp: int


class PriceLevel(BaseModel):
    price: float
    qty: float


class Orderbook(BaseModel):
    exchange: str
    symbol: str
    timestamp: int
    bids: list[PriceLevel]
    asks: list[PriceLevel]


class Liquidation(BaseModel):
    exchange: str
    symbol: str
    side: str
    price: float
    qty: float
    quoteQty: float
    timestamp: int
