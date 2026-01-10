"""
Tree of Alpha news feed client.
"""
import asyncio
import json
import logging
import time
from typing import Callable, Optional
from collections import deque
import websockets

logger = logging.getLogger(__name__)

TREE_OF_ALPHA_URL = "wss://news.treeofalpha.com/ws"
HEARTBEAT_INTERVAL = 10  # seconds
MAX_HISTORY = 100  # Keep last 100 news items


class NewsClient:
    def __init__(self):
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.running = False
        self.on_news: Optional[Callable] = None
        self.news_history: deque = deque(maxlen=MAX_HISTORY)

    async def start(self):
        self.running = True
        asyncio.create_task(self._run_ws())

    async def stop(self):
        self.running = False
        if self.ws:
            await self.ws.close()

    async def _run_ws(self):
        delay = 5
        max_delay = 60

        while self.running:
            try:
                logger.info(f"Connecting to Tree of Alpha: {TREE_OF_ALPHA_URL}")
                async with websockets.connect(TREE_OF_ALPHA_URL, ping_interval=None) as ws:
                    self.ws = ws
                    logger.info("Connected to Tree of Alpha")
                    delay = 5

                    # Start heartbeat task
                    heartbeat_task = asyncio.create_task(self._heartbeat())

                    try:
                        async for msg in ws:
                            await self._handle_message(msg)
                    finally:
                        heartbeat_task.cancel()

            except Exception as e:
                logger.error(f"Tree of Alpha WebSocket error: {e}")

            logger.warning("Tree of Alpha disconnected, reconnecting...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)

    async def _heartbeat(self):
        while self.running:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                if self.ws:
                    await self.ws.send("ping")
            except Exception as e:
                logger.debug(f"Heartbeat error: {e}")
                break

    async def _handle_message(self, msg: str):
        if msg == "pong":
            return

        try:
            data = json.loads(msg)
        except json.JSONDecodeError:
            return

        news = self._transform_message(data)
        if news:
            self.news_history.append(news)
            if self.on_news:
                await self.on_news(news)

    def _transform_message(self, raw: dict) -> Optional[dict]:
        news_id = raw.get("_id", "")
        title = raw.get("title", "")
        body = raw.get("body", "")

        if not news_id and not title and not body:
            return None

        # Tree of Alpha sends title in body field sometimes
        if body and not raw.get("source"):
            title = body
            body = ""

        # Parse timestamp
        raw_time = raw.get("time", 0)
        timestamp = raw_time if raw_time > 0 else int(time.time() * 1000)

        # Get URL (could be in url or link field)
        url = raw.get("url", "") or raw.get("link", "")

        # Parse symbols (can be array of strings or array of objects)
        symbols = self._parse_symbols(raw.get("symbols"))

        return {
            "id": news_id,
            "source": "treeofalpha",
            "title": title,
            "body": body,
            "url": url,
            "timestamp": timestamp,
            "symbols": symbols,
        }

    def _parse_symbols(self, data) -> list:
        if not data:
            return []

        if isinstance(data, list):
            if len(data) == 0:
                return []
            # Check if it's array of strings or array of objects
            if isinstance(data[0], str):
                return data
            elif isinstance(data[0], dict):
                return [s.get("symbol", "") for s in data if s.get("symbol")]

        return []

    def get_history(self) -> list:
        return list(self.news_history)


news_client = NewsClient()
