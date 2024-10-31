# src/services/market_data.py

from typing import Set, Dict, Optional, List, Tuple
import asyncio
from datetime import datetime, timezone

from src.config import MarketDataConfig

from src.services.base import ServiceBase
from src.utils.time import get_current_timestamp
from ..repositories.market_data import SymbolRepository, KlineRepository
from ..adapters.registry import ExchangeAdapterRegistry
from ..utils.domain_types import Timeframe, ExchangeName, SymbolName, Timestamp, ServiceStatus
from ..core.exceptions import ServiceError
from ..utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class MarketDataService(ServiceBase):
    """Core market data service"""

    def __init__(self,
                 symbol_repository: SymbolRepository,
                 kline_repository: KlineRepository,
                 exchange_registry: ExchangeAdapterRegistry,
                 config: MarketDataConfig):
        super().__init__()
        self.symbol_repository = symbol_repository
        self.kline_repository = kline_repository
        self.exchange_registry = exchange_registry

        # Service state
        self._status = ServiceStatus.STOPPED
        self._active_timeframes: Set[Timeframe] = set(config.default_timeframes)
        self._sync_tasks: Dict[str, asyncio.Task] = {}
        self._last_error: Optional[Exception] = None

        # Configuration
        self._sync_interval = config.sync_interval
        self._retry_interval = config.retry_interval
        self._max_retries = config.max_retries

    @property
    def is_healthy(self) -> bool:
        """Check if service is healthy"""
        return self._status == ServiceStatus.RUNNING and not self._last_error

    async def start(self) -> None:
        """Start market data service"""
        try:
            self._status = ServiceStatus.STARTING
            logger.info("Starting market data service")

            # Initialize exchange adapters
            await self.exchange_registry.initialize_all()

            # Start data synchronization
            for exchange in self.exchange_registry.get_registered():
                await self._start_sync_task(exchange)

            self._status = ServiceStatus.RUNNING
            logger.info("Market data service started successfully")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            self._last_error = e
            logger.error(f"Failed to start market data service: {e}")
            raise ServiceError(f"Service start failed: {str(e)}")

    async def stop(self) -> None:
        """Stop market data service"""
        try:
            self._status = ServiceStatus.STOPPING
            logger.info("Stopping market data service")

            # Cancel all sync tasks
            for task in self._sync_tasks.values():
                task.cancel()
            await asyncio.gather(*self._sync_tasks.values(), return_exceptions=True)
            self._sync_tasks.clear()

            # Close exchange connections
            await self.exchange_registry.close_all()

            self._status = ServiceStatus.STOPPED
            logger.info("Market data service stopped")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            self._last_error = e
            logger.error(f"Error stopping market data service: {e}")
            raise ServiceError(f"Service stop failed: {str(e)}")

    async def add_timeframe(self, timeframe: Timeframe) -> None:
        """Add new timeframe support"""
        if timeframe in self._active_timeframes:
            return

        self._active_timeframes.add(timeframe)
        logger.info(f"Added support for timeframe: {timeframe.value}")

    async def _start_sync_task(self, exchange: ExchangeName) -> None:
        """Start synchronization task for an exchange"""
        if exchange in self._sync_tasks:
            return

        task = asyncio.create_task(
            self._sync_loop(exchange),
            name=f"sync_{exchange}"
        )
        self._sync_tasks[exchange] = task
        logger.info(f"Started sync task for exchange: {exchange}")

    async def _sync_loop(self, exchange: ExchangeName) -> None:
        """Main synchronization loop for an exchange"""
        retry_count = 0

        while True:
            try:
                # Get exchange adapter
                adapter = self.exchange_registry.get_adapter(exchange)

                # Get active symbols
                symbols = await adapter.get_symbols()

                # Sync each symbol
                for symbol_info in symbols:
                    for timeframe in self._active_timeframes:
                        await self._sync_symbol_data(
                            symbol_info.name,
                            timeframe,
                            exchange
                        )

                # Reset retry count on success
                retry_count = 0
                await asyncio.sleep(self._sync_interval)

            except asyncio.CancelledError:
                logger.info(f"Sync task cancelled for {exchange}")
                break

            except Exception as e:
                retry_count += 1
                self._last_error = e
                logger.error(f"Error in sync loop for {exchange}: {e}")

                if retry_count >= self._max_retries:
                    logger.error(f"Max retries reached for {exchange}")
                    self._status = ServiceStatus.ERROR
                    break

                await asyncio.sleep(self._retry_interval)

    async def _sync_symbol_data(self,
                              symbol: SymbolName,
                              timeframe: Timeframe,
                              exchange: ExchangeName) -> None:
        """Sync data for a specific symbol and timeframe"""
        try:
            # Get or create symbol record
            symbol_record = await self.symbol_repository.get_or_create(
                symbol, exchange
            )

            # Get latest timestamp
            latest_ts = await self.kline_repository.get_latest_timestamp(
                symbol, timeframe, exchange
            )

            # Check if update needed
            current_time = get_current_timestamp()
            if (current_time - latest_ts) < timeframe.to_milliseconds():
                return

            # Get new data
            adapter = self.exchange_registry.get_adapter(exchange)
            klines = await adapter.get_klines(
                symbol, timeframe, latest_ts
            )

            if klines:
                # Insert new data
                inserted = await self.kline_repository.insert_batch(
                    symbol_record.id,
                    timeframe,
                    [k.to_tuple() for k in klines]
                )
                logger.info(
                    f"Inserted {inserted} klines for {symbol} "
                    f"{timeframe.value}"
                )

                # Check for gaps
                end_time = Timestamp(max(k.timestamp for k in klines))
                gaps = await self.kline_repository.get_data_gaps(
                    symbol,
                    timeframe,
                    exchange,
                    latest_ts,
                    end_time
                )

                if gaps:
                    await self._handle_data_gaps(
                        symbol,
                        timeframe,
                        exchange,
                        gaps
                    )

        except Exception as e:
            logger.error(f"Error syncing {symbol} {timeframe.value}: {e}")
            raise ServiceError(
                f"Symbol sync failed: {symbol} {timeframe.value}: {str(e)}"
            )

    async def _handle_data_gaps(self,
                              symbol: SymbolName,
                              timeframe: Timeframe,
                              exchange: ExchangeName,
                              gaps: List[Tuple[Timestamp, Timestamp]]) -> None:
        """Handle detected data gaps"""
        adapter = self.exchange_registry.get_adapter(exchange)
        symbol_record = await self.symbol_repository.get_or_create(
            symbol, exchange
        )

        for start, end in gaps:
            try:
                klines = await adapter.get_klines(
                    symbol, timeframe, start
                )
                if klines:
                    await self.kline_repository.insert_batch(
                        symbol_record.id,
                        timeframe,
                        [k.to_tuple() for k in klines]
                    )
                    logger.info(
                        f"Filled gap for {symbol} {timeframe.value} "
                        f"from {start} to {end}"
                    )
            except Exception as e:
                logger.error(f"Failed to fill gap for {symbol}: {e}")

    async def handle_error(self, error: Optional[Exception]) -> None:
        """Handle service errors"""
        if error is None:
            return

        if isinstance(error, ServiceError):
            # Try to recover service
            try:
                logger.info("Attempting service recovery")
                await self.stop()
                await asyncio.sleep(self._retry_interval)
                await self.start()
                self._last_error = None
                logger.info("Service recovered successfully")
            except Exception as e:
                logger.error(f"Recovery failed: {e}")
                raise ServiceError("Service recovery failed")
        else:
            logger.error(f"Unhandled error: {error}")
            raise error