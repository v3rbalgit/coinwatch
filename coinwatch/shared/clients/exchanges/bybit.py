# src/adapters/bybit.py

from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Callable, Coroutine
import aiohttp
import asyncio
import json
import time

from ..registry import ExchangeAdapter
from shared.core.config import BybitConfig
from shared.core.exceptions import AdapterError
from shared.core.models import KlineData, SymbolInfo
from shared.utils.domain_types import Timeframe
from shared.utils.logger import LoggerSetup
from shared.utils.rate_limit import RateLimiter
from shared.utils.retry import RetryConfig, RetryStrategy
from shared.utils.time import TimeUtils

logger = LoggerSetup.setup(__name__)

class BybitAdapter(ExchangeAdapter):
    """
    Async Bybit API adapter using aiohttp.

    Handles:
    - Rate limiting with circuit breaker
    - Connection management
    - Retry logic
    - Websocket streaming for real-time data
    """

    BASE_URL = "https://api.bybit.com"
    TESTNET_URL = "https://api-testnet.bybit.com"
    WS_URL = "wss://stream.bybit.com/v5/public/linear"
    TESTNET_WS_URL = "wss://stream-testnet.bybit.com/v5/public/linear"

    # Class-level variables for circuit breaker
    _rate_limit_reset: Optional[int] = None  # Shared timestamp when rate limit resets (in milliseconds)
    _circuit_breaker_lock = asyncio.Lock()   # Lock for thread-safe access

    def __init__(self, config: BybitConfig):
        super().__init__()

        self._config = config or BybitConfig()

        # Base URL based on testnet setting
        self._base_url = self.TESTNET_URL if self._config.testnet else self.BASE_URL
        self._ws_url = self.TESTNET_WS_URL if self._config.testnet else self.WS_URL

        # Initialize rate limiter
        self._rate_limiter = RateLimiter(
            calls_per_window=self._config.rate_limit,
            window_size=self._config.rate_limit_window
        )

        # Configure retry strategy
        self._retry_strategy = RetryStrategy(RetryConfig(
            base_delay=1.0,
            max_delay=30.0,
            max_retries=3,
            jitter_factor=0.1
        ))

        # Websocket management
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._subscribed_topics: Set[str] = set()
        self._kline_handlers: Dict[str, Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]] = {}
        self._ws_lock = asyncio.Lock()
        self._ws_connected = asyncio.Event()
        self._ws_reconnect_delay = 1.0
        self._ws_heartbeat_interval = 30.0
        self._last_heartbeat = 0.0

    async def _create_session(self) -> aiohttp.ClientSession:
        """Create new session with Bybit configuration"""
        return aiohttp.ClientSession(
            base_url=self._base_url,
            timeout=aiohttp.ClientTimeout(total=30),
            headers={'Content-Type': 'application/json'}
        )

    async def _request(self,
                      method: str,
                      endpoint: str,
                      **kwargs: Any) -> Any:
        """
        Make API request with retry logic and rate limiting

        Args:
            method: HTTP method
            endpoint: API endpoint
            **kwargs: Additional request parameters
        """
        attempt = 0
        session = await self._get_session()

        while True:
            try:
                # Handle rate limiting
                await self._rate_limiter.acquire()

                # Check circuit breaker
                async with self.__class__._circuit_breaker_lock:
                    current_time = int(time.time() * 1000)
                    if self.__class__._rate_limit_reset and current_time < self.__class__._rate_limit_reset:
                        sleep_time = (self.__class__._rate_limit_reset - current_time) / 1000
                        logger.warning(f"Rate limit active! Sleeping for {sleep_time:.2f}s")
                        await asyncio.sleep(sleep_time)

                async with session.request(method, endpoint, **kwargs) as response:
                    if response.status == 429:  # Rate limit exceeded
                        reset_timestamp = int(response.headers.get('X-Bapi-Limit-Reset-Timestamp', 0))
                        async with self.__class__._circuit_breaker_lock:
                            self.__class__._rate_limit_reset = reset_timestamp
                            wait_time = (reset_timestamp - current_time) / 1000
                            logger.warning(
                                f"Rate limit hit! Next retry in {wait_time:.2f}s. "
                                f"Endpoint: {endpoint}"
                            )
                            await asyncio.sleep(wait_time)
                            continue

                    response.raise_for_status()
                    data = await response.json()

                    # Handle Bybit-specific error responses
                    if data.get('retCode') != 0:
                        raise AdapterError(
                            f"API error {data.get('retCode')}: {data.get('retMsg')}"
                        )

                    # Clear rate limit reset after successful request
                    async with self.__class__._circuit_breaker_lock:
                        self.__class__._rate_limit_reset = None

                    return data.get('result', {})

            except aiohttp.ClientError as e:
                should_retry, reason = self._retry_strategy.should_retry(attempt, e)
                if should_retry:
                    attempt += 1
                    delay = self._retry_strategy.get_delay(attempt)
                    logger.warning(
                        f"API request failed ({reason}), "
                        f"retry {attempt} after {delay:.2f}s: {str(e)}"
                    )
                    await asyncio.sleep(delay)
                    continue

                raise AdapterError(f"API request failed: {str(e)}")

    async def get_symbols(self, symbol: Optional[str] = None) -> List[SymbolInfo]:
        """Get available trading pairs"""
        try:
            params = {'category': 'linear'}

            if symbol:
                params['symbol'] = symbol

            data = await self._request(
                'GET',
                '/v5/market/instruments-info',
                params=params
            )

            symbols: List[SymbolInfo] = []
            for item in data.get('list', []):
                if (item.get('status') == 'Trading' and
                    'USDT' in item.get('symbol', '')):

                    symbols.append(SymbolInfo(
                        name=item['symbol'],
                        base_asset=item['baseCoin'],
                        quote_asset=item['quoteCoin'],
                        price_precision=str(item['priceFilter']['tickSize']).count('0'),
                        qty_precision=str(item['lotSizeFilter']['qtyStep']).count('0'),
                        min_order_qty=Decimal(str(item['lotSizeFilter']['minOrderQty'])),
                        launch_time=int(item.get('launchTime', '0'))
                    ))

            logger.info(f"Fetched {len(symbols)} active USDT pairs")
            return symbols

        except Exception as e:
            logger.error(f"Failed to fetch symbols: {str(e)}")
            raise

    async def get_klines(self,
                        symbol: SymbolInfo,
                        timeframe: Timeframe,
                        start_time: Optional[int] = None,
                        end_time: Optional[int] = None,
                        limit: Optional[int] = None) -> List[KlineData]:
        """Get kline (candlestick) data"""
        try:
            params = {
                "category": "linear",
                "symbol": symbol.name,
                "interval": timeframe.value,
                "limit": limit or self._config.kline_limit
            }

            if start_time is not None:
                params['start'] = start_time
            if end_time is not None:
                params['end'] = end_time

            data = await self._request(
                'GET',
                '/v5/market/kline',
                params=params
            )

            klines: List[KlineData] = []
            current_time = TimeUtils.get_current_timestamp()

            # Process klines in ascending order
            for item in reversed(data.get('list', [])):
                timestamp = int(item[0])

                if timestamp > current_time:
                    continue

                if end_time and timestamp > end_time:
                    continue
                if start_time and timestamp < start_time:
                    continue

                klines.append(KlineData(
                    timestamp=timestamp,
                    open_price=Decimal(str(item[1])),
                    high_price=Decimal(str(item[2])),
                    low_price=Decimal(str(item[3])),
                    close_price=Decimal(str(item[4])),
                    volume=Decimal(str(item[5])),
                    turnover=Decimal(str(item[6])),
                    symbol=symbol,
                    timeframe=timeframe
                ))

            return klines

        except Exception as e:
            logger.error(f"Failed to fetch klines for {symbol}: {str(e)}")
            raise

    async def subscribe_klines(self,
                             symbol: SymbolInfo,
                             timeframe: Timeframe,
                             handler: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]) -> None:
        """
        Subscribe to real-time kline updates for a symbol.

        Args:
            symbol: Trading pair to subscribe to
            timeframe: Kline interval
            handler: Async callback for handling kline updates
        """
        topic = f"kline.{timeframe.value}.{symbol.name}"

        if self._ws:
            async with self._ws_lock:
                if not self._ws_connected.is_set():
                    await self._connect_websocket()

                if topic not in self._subscribed_topics:
                    await self._ws.send_str(json.dumps({
                        "op": "subscribe",
                        "args": [topic]
                    }))
                    self._subscribed_topics.add(topic)
                    self._kline_handlers[topic] = handler
                    logger.info(f"Subscribed to {topic}")

    async def unsubscribe_klines(self,
                                symbol: SymbolInfo,
                                timeframe: Timeframe) -> None:
        """
        Unsubscribe from kline updates for a symbol.

        Args:
            symbol: Trading pair to unsubscribe from
            timeframe: Kline interval
        """
        topic = f"kline.{timeframe.value}.{symbol.name}"

        if self._ws:
            async with self._ws_lock:
                if topic in self._subscribed_topics:
                    await self._ws.send_str(json.dumps({
                        "op": "unsubscribe",
                        "args": [topic]
                    }))
                    self._subscribed_topics.remove(topic)
                    self._kline_handlers.pop(topic, None)
                    logger.info(f"Unsubscribed from {topic}")

    async def _connect_websocket(self) -> None:
        """Establish websocket connection"""
        try:
            session = await self._get_session()
            self._ws = await session.ws_connect(self._ws_url)
            self._ws_connected.set()
            self._ws_task = asyncio.create_task(self._handle_websocket())
            self._last_heartbeat = time.time()
            logger.info("Websocket connected")

        except Exception as e:
            logger.error(f"Failed to connect websocket: {e}")
            self._ws_connected.clear()
            raise

    async def _handle_websocket(self) -> None:
        """Handle websocket messages and maintain connection"""
        if self._ws:
            try:
                while True:
                    msg = await self._ws.receive()

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)

                        if 'topic' in data:  # Kline update
                            topic = data['topic']
                            if handler := self._kline_handlers.get(topic):
                                await handler(data)

                        elif data.get('op') == 'ping':  # Heartbeat
                            await self._ws.send_str(json.dumps({"op": "pong"}))
                            self._last_heartbeat = time.time()

                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break

                    # Check heartbeat
                    if time.time() - self._last_heartbeat > self._ws_heartbeat_interval * 2:
                        logger.warning("Websocket heartbeat timeout")
                        break

            except Exception as e:
                logger.error(f"Websocket error: {e}")

            finally:
                self._ws_connected.clear()
                await self._handle_disconnect()

    async def _handle_disconnect(self) -> None:
        """Handle websocket disconnection and reconnection"""
        try:
            if self._ws:
                await self._ws.close()

            # Exponential backoff for reconnect
            await asyncio.sleep(self._ws_reconnect_delay)
            self._ws_reconnect_delay = min(self._ws_reconnect_delay * 2, 60)

            # Reconnect and resubscribe
            await self._connect_websocket()
            if self._ws:
                for topic in list(self._subscribed_topics):
                    await self._ws.send_str(json.dumps({
                        "op": "subscribe",
                        "args": [topic]
                    }))

            # Reset reconnect delay on successful reconnection
            self._ws_reconnect_delay = 1.0

        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            asyncio.create_task(self._handle_disconnect())

    async def cleanup(self) -> None:
        """Cleanup websocket resources"""
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass

        if self._ws:
            await self._ws.close()

        self._subscribed_topics.clear()
        self._kline_handlers.clear()
        self._ws_connected.clear()
