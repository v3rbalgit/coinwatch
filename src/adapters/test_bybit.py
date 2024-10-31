import asyncio
from asyncio import Lock
import time
from typing import List, Optional, Dict, Any
from decimal import Decimal
from pybit.unified_trading import HTTP

from config import BybitConfig
from ..core.models import KlineData, SymbolInfo
from ..core.protocols import ExchangeAdapter
from ..utils.domain_types import Timeframe, SymbolName
from ..utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class BybitTestAdapter(ExchangeAdapter):
    """Bybit exchange adapter with test symbol filtering"""

    def __init__(self, config: Optional[BybitConfig] = None):
        self.session = HTTP(
            testnet=config.testnet if config else False
        )
        self._initialized = False

        # Set rate limits from config or use defaults
        self.RATE_LIMIT = config.rate_limit if config else 600
        self.WINDOW_SIZE = config.rate_limit_window if config else 300

        # Store credentials for future use
        self._api_key = config.api_key if config else None
        self._api_secret = config.api_secret if config else None


        # Initialize rate limiting
        self._lock = Lock()
        self._tokens = self.RATE_LIMIT
        self._last_refill = time.time()

    async def _acquire_token(self) -> bool:
        """Rate limiting logic remains the same"""
        async with self._lock:
            current_time = time.time()
            elapsed = current_time - self._last_refill

            if elapsed >= self.WINDOW_SIZE:
                self._tokens = self.RATE_LIMIT
                self._last_refill = current_time
                logger.debug("Rate limit token bucket refilled")

            if self._tokens > 0:
                self._tokens -= 1
                return True
            return False

    async def _handle_rate_limit(self) -> None:
        """Rate limit handling remains the same"""
        while not await self._acquire_token():
            sleep_time = self.WINDOW_SIZE - (time.time() - self._last_refill)
            if sleep_time > 0:
                logger.debug(f"Rate limit reached, waiting {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)

    def _validate_response(self, response: Any) -> Dict[str, Any]:
        """Response validation remains the same"""
        if isinstance(response, tuple):
            response = response[0]

        if not isinstance(response, dict):
            raise ValueError(f"Invalid response type: {type(response)}")

        ret_code = response.get('retCode')
        if ret_code is None:
            raise ValueError("Response missing retCode")

        if ret_code != 0:
            raise ValueError(f"API Error: {ret_code} - {response.get('retMsg')}")

        return response

    async def get_symbols(self) -> List[SymbolInfo]:
        """Get available trading pairs, filtered by test symbols"""
        try:
            await self._handle_rate_limit()
            response = await asyncio.to_thread(
                self.session.get_instruments_info,
                category='linear'
            )

            response = self._validate_response(response)

            symbols: List[SymbolInfo] = []
            for item in response.get('result', {}).get('list', []):
                if (isinstance(item, dict) and
                    item.get('status') == 'Trading' and
                    'USDT' in item.get('symbol', '')):

                    symbols.append(SymbolInfo(
                        name=item['symbol'],
                        base_asset=item['baseCoin'],
                        quote_asset=item['quoteCoin'],
                        price_precision=str(item['priceFilter']['tickSize']).count('0'),
                        qty_precision=str(item['lotSizeFilter']['qtyStep']).count('0'),
                        min_order_qty=Decimal(str(item['lotSizeFilter']['minOrderQty']))
                    ))

            logger.info(f"Fetched {len(symbols)} test symbols")
            return symbols

        except Exception as e:
            logger.error(f"Failed to fetch symbols: {e}")
            raise

    async def get_klines(self,
                        symbol: SymbolName,
                        timeframe: Timeframe,
                        start_time: Optional[int] = None,
                        limit: int = 200) -> List[KlineData]:
        """Fetch kline data from Bybit"""
        try:
            await self._handle_rate_limit()

            params = {
                "category": "linear",
                "symbol": symbol,
                "interval": timeframe.value,
                "limit": limit
            }

            if start_time is not None:
                params["start"] = start_time

            logger.debug(f"Requesting klines for {symbol}: {params}")
            response = await asyncio.to_thread(
                self.session.get_kline,
                **params
            )

            response = self._validate_response(response)

            # Add debug logging
            logger.debug(f"Raw kline response for {symbol}: {response}")

            klines: List[KlineData] = []
            for item in response.get('result', {}).get('list', []):
                # Debug log each item
                logger.debug(f"Processing kline item: {item}")
                klines.append(KlineData(
                    timestamp=int(item[0]),
                    open_price=Decimal(item[1]),
                    high_price=Decimal(item[2]),
                    low_price=Decimal(item[3]),
                    close_price=Decimal(item[4]),
                    volume=Decimal(item[5]),
                    turnover=Decimal(item[6]),
                    symbol=symbol,
                    timeframe=timeframe.value
                ))

            return klines

        except Exception as e:
            logger.error(f"Failed to fetch klines for {symbol}: {e}")
            raise

    async def get_latest_price(self, symbol: SymbolName) -> Decimal:
        """Get latest price for symbol"""
        try:
            await self._handle_rate_limit()
            response = await asyncio.to_thread(
                self.session.get_tickers,
                category="linear",
                symbol=symbol
            )

            response = self._validate_response(response)

            ticker = response.get('result', {}).get('list', [{}])[0]
            return Decimal(str(ticker.get('lastPrice', '0')))

        except Exception as e:
            logger.error(f"Failed to fetch latest price for {symbol}: {e}")
            raise

    async def close(self) -> None:
        """Cleanup any resources"""
        self._initialized = False
        logger.info("BybitAdapter closed")