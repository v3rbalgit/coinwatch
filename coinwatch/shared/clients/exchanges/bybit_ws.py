import asyncio
import json
from typing import Any, Dict, Optional, Callable, Coroutine
from websockets.asyncio.client import connect, ClientConnection
from websockets.exceptions import ConnectionClosed

from shared.core.enums import Timeframe
from shared.core.models import SymbolInfo
from shared.core.config import BybitConfig
from shared.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)


class BybitWebsocket:
    """Simplified Bybit websocket client for real-time data streaming"""

    WS_URL = "wss://stream.bybit.com/v5/public/linear"
    TESTNET_WS_URL = "wss://stream-testnet.bybit.com/v5/public/linear"

    def __init__(self, config: BybitConfig):
        self._config = config
        self._ws: Optional[ClientConnection] = None
        self._ws_url = self.TESTNET_WS_URL if self._config.testnet else self.WS_URL
        self._ws_lock = asyncio.Lock()
        self._runner: Optional[asyncio.Task] = None
        self._handlers: Dict[str, Callable] = {}

    async def start(self) -> None:
        """Start websocket client"""
        self._runner = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop websocket client"""
        if self._runner:
            self._runner.cancel()
            try:
                await self._runner
            except asyncio.CancelledError:
                pass
        self._runner = None
        self._handlers.clear()

    async def _run(self) -> None:
        """Main websocket loop using connect as async iterator"""
        async for websocket in connect(
            self._ws_url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
            compression=None,
            max_size=2**23,
            max_queue=1000
        ):
            try:
                async with self._ws_lock:
                    self._ws = websocket
                    logger.info(f"Connected to {self._ws_url}")

                    # Subscribe to all pending topics
                    if self._handlers:
                        subscribe_msg = { "op": "subscribe", "args": list(self._handlers.keys()) }
                        await websocket.send(json.dumps(subscribe_msg))
                        logger.debug(f"Resubscribed to topics: {list(self._handlers.keys())}")

                await self._process_messages(websocket)

            except ConnectionClosed:
                logger.info("Connection closed, reconnecting...")
                self._ws = None
                continue
            except Exception as e:
                logger.error(f"Websocket error: {e}")
                self._ws = None
                continue

    async def _process_messages(self, websocket: ClientConnection) -> None:
        """Handle incoming websocket messages"""
        async for message in websocket:
            try:
                data = json.loads(message)

                # Handle subscription responses
                if 'op' in data:
                    if data['op'] in ('subscribe', 'unsubscribe'):
                        success = data.get('success', False)
                        if success:
                            logger.debug(f"Successfully {data['op']}d to topics")
                        else:
                            logger.error(f"Failed to {data['op']}: {data.get('ret_msg')}")
                        continue

                # Handle kline updates
                if 'topic' in data and data.get('data'):
                    topic = data['topic']
                    if handler := self._handlers.get(topic):
                        # Only process if it's a confirmed candle
                        kline_data = data['data'][0]
                        if kline_data.get('confirm', False):
                            await handler(data)

            except Exception as e:
                logger.error(f"Error processing message: {e}")

    async def subscribe_klines(self,
                             symbol: SymbolInfo,
                             timeframe: Timeframe,
                             handler: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]) -> None:
        """Subscribe to kline updates"""
        topic = f"kline.{timeframe.value}.{symbol.name}"

        if not self._runner:
            await self.start()

        self._handlers[topic] = handler

        # Send subscription through existing connection
        async with self._ws_lock:
            if self._ws:
                subscribe_msg = { "op": "subscribe", "args": [topic] }
                await self._ws.send(json.dumps(subscribe_msg))

    async def unsubscribe_klines(self,
                                symbol: SymbolInfo,
                                timeframe: Timeframe) -> None:
        """Unsubscribe from kline updates"""
        topic = f"kline.{timeframe.value}.{symbol.name}"

        if topic in self._handlers:
            self._handlers.pop(topic)

            async with self._ws_lock:
                if self._ws:
                    unsubscribe_msg = { "op": "unsubscribe", "args": [topic] }
                    await self._ws.send(json.dumps(unsubscribe_msg))