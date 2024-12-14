import asyncio
from typing import Any, Dict, Set
from decimal import Decimal

from shared.core.models import KlineData, SymbolInfo
from shared.database.repositories.kline import KlineRepository
from shared.database.repositories.symbol import SymbolRepository
from shared.messaging.broker import MessageBroker
from shared.messaging.schemas import (
    MessageType,
    KlineMessage,
    ErrorMessage,
    CollectionMessage
)
from shared.utils.domain_types import Timeframe
from shared.utils.logger import LoggerSetup
from shared.utils.progress import MarketDataProgress
from shared.utils.time import TimeUtils
from shared.clients.registry import ExchangeAdapterRegistry

logger = LoggerSetup.setup(__name__)


class DataCollector:
    """
    Handles market data collection and real-time streaming.

    Responsibilities:
    - Collects historical data for new symbols
    - Fills gaps in existing data
    - Manages real-time data streaming via websocket
    """

    def __init__(self,
                 adapter_registry: ExchangeAdapterRegistry,
                 symbol_repository: SymbolRepository,
                 kline_repository: KlineRepository,
                 message_broker: MessageBroker,
                 base_timeframe: Timeframe = Timeframe.MINUTE_5,
                 batch_size: int = 1000):
        # Core dependencies
        self._adapter_registry = adapter_registry
        self._symbol_repository = symbol_repository
        self._kline_repository = kline_repository
        self._message_broker = message_broker
        self._base_timeframe = base_timeframe
        self._batch_size = batch_size

        # State management
        self._processing_symbols: Set[SymbolInfo] = set()
        self._streaming_symbols: Set[SymbolInfo] = set()
        self._collection_lock = asyncio.Lock()
        self._symbol_progress: Dict[SymbolInfo, MarketDataProgress] = {}

    async def collect(self, symbol: SymbolInfo, start_time: int, end_time: int, context: Dict[str, Any]) -> None:
        """Start data collection for a symbol"""
        async with self._collection_lock:
            if symbol not in self._processing_symbols:
                self._processing_symbols.add(symbol)
                try:
                    # Initialize progress tracking
                    self._symbol_progress[symbol] = MarketDataProgress(
                        symbol=symbol,
                        start_time=TimeUtils.get_current_datetime()
                    )

                    await self._stop_streaming(symbol)

                    # Ensure symbol exists and continue from latest timestamp
                    await self._symbol_repository.get_or_create(symbol)

                    latest_timestamp = await self._kline_repository.get_latest_timestamp(symbol)
                    if latest_timestamp > symbol.launch_time:
                        start_time = latest_timestamp

                    # Process collection
                    await self._process_collection(symbol, start_time, end_time)

                    await self._start_streaming(symbol)

                    # Log completion
                    if progress := self._symbol_progress.get(symbol):
                        logger.info(progress.get_completion_summary(TimeUtils.get_current_datetime()))

                    # Publish completion
                    await self._message_broker.publish(
                        MessageType.COLLECTION_COMPLETE,
                        CollectionMessage(service="market_data",
                                          type=MessageType.COLLECTION_COMPLETE,
                                          timestamp=TimeUtils.get_current_timestamp(),
                                          timeframe=self._base_timeframe.value,
                                          symbol=symbol.name,
                                          exchange=symbol.exchange,
                                          start_time=start_time,
                                          end_time=end_time,
                                          processed=self._symbol_progress[symbol].processed_candles,
                                          context=context
                                          ).model_dump())
                except Exception as e:
                    await self._publish_error("CollectionError", str(e), symbol, start_time, end_time, context)
                    raise
                finally:
                    self._processing_symbols.remove(symbol)
                    self._symbol_progress.pop(symbol, None)

    async def _process_collection(self, symbol: SymbolInfo, start_time: int, end_time: int) -> None:
        """Core collection logic for both historical and gap filling"""
        interval_ms = self._base_timeframe.to_milliseconds()
        current_time = TimeUtils.get_current_timestamp()

        # Align timestamps
        aligned_start = start_time - (start_time % interval_ms)
        aligned_end = min(
            end_time - (end_time % interval_ms),
            current_time - (current_time % interval_ms) - interval_ms  # Last completed interval
        )

        # Calculate total candles for progress tracking
        total_candles = ((aligned_end - aligned_start) // interval_ms) + 1
        processed_candles = 0

        # Update progress with total
        if progress := self._symbol_progress.get(symbol):
            progress.update(processed_candles, total_candles)

        # Process in batches
        current_start = aligned_start
        while current_start < aligned_end:
            batch_end = min(aligned_end, current_start + (self._batch_size * interval_ms))

            # Get and store batch
            adapter = self._adapter_registry.get_adapter(symbol.exchange)
            klines = await adapter.get_klines(
                symbol=symbol,
                timeframe=self._base_timeframe,
                start_time=current_start,
                end_time=batch_end,
                limit=self._batch_size
            )

            if not klines:
                break

            # Filter for completed intervals
            klines = [k for k in klines if TimeUtils.is_complete_interval(k.timestamp, interval_ms)]
            if klines:
                # Store batch
                processed = await self._kline_repository.insert_batch(
                    symbol,
                    self._base_timeframe,
                    [k.to_tuple() for k in klines]
                )

                # Update progress
                processed_candles += processed
                if progress := self._symbol_progress.get(symbol):
                    progress.update(processed_candles)
                    logger.info(progress)

                # Refresh aggregates
                await self._refresh_continuous_aggregate(
                    self._base_timeframe,
                    klines[0].timestamp,
                    klines[-1].timestamp + interval_ms
                )

                current_start = klines[-1].timestamp + interval_ms
            else:
                current_start = batch_end

    async def delist(self, symbol: SymbolInfo) -> None:
        """Handle symbol delisting"""
        try:
            # Stop streaming if active
            if symbol in self._streaming_symbols:
                adapter = self._adapter_registry.get_adapter(symbol.exchange)
                await adapter.unsubscribe_klines(symbol, self._base_timeframe)
                self._streaming_symbols.remove(symbol)
                logger.info(f"Stopped streaming for delisted symbol {symbol}")

            # Remove from collection if in progress
            async with self._collection_lock:
                if symbol in self._processing_symbols:
                    self._processing_symbols.remove(symbol)
                    logger.info(f"Removed delisted symbol from collection {symbol}")

            # Clean up data
            await self._symbol_repository.delete_symbol(symbol)
            await self._kline_repository.delete_symbol_data(symbol)
            logger.info(f"Cleaned up data for delisted symbol {symbol}")

        except Exception as e:
            logger.error(f"Error handling symbol delisting for {symbol}: {e}")

    async def _publish_kline_update(self, symbol: SymbolInfo, kline: KlineData) -> None:
        """Publish kline update message"""
        await self._message_broker.publish(
            MessageType.KLINE_UPDATED,
            KlineMessage(
                service="market_data",
                type=MessageType.KLINE_UPDATED,
                timestamp=TimeUtils.get_current_timestamp(),
                symbol=symbol.name,
                exchange=symbol.exchange,
                timeframe=str(self._base_timeframe),
                kline_timestamp=kline.timestamp,
                open_price=float(kline.open_price),
                high_price=float(kline.high_price),
                low_price=float(kline.low_price),
                close_price=float(kline.close_price),
                volume=float(kline.volume),
                turnover=float(kline.turnover)
            ).model_dump()
        )

    async def _publish_error(self, error_type: str, message: str, symbol: SymbolInfo,
                           start_time: int, end_time: int, context: Dict[str, Any]) -> None:
        """Publish error message"""
        await self._message_broker.publish(
            MessageType.ERROR_REPORTED,
            ErrorMessage(
                service="market_data",
                type=MessageType.ERROR_REPORTED,
                timestamp=TimeUtils.get_current_timestamp(),
                error_type=error_type,
                severity="error",
                message=message,
                context={
                    "symbol": symbol.name,
                    "exchange": symbol.exchange,
                    "start_time": start_time,
                    "end_time": end_time,
                    "collection_type": context.get("type"),
                    "batch_size": self._batch_size,
                    "timeframe": self._base_timeframe.value
                }
            ).model_dump()
        )

    async def _handle_kline_update(self, symbol: SymbolInfo, data: Dict[str, Any]) -> None:
        """Handle real-time kline updates from websocket"""
        try:
            kline_data = data.get('data', [{}])[0]
            if not kline_data.get('confirm', False):
                return  # Skip unconfirmed candles

            kline = KlineData(
                timestamp=int(kline_data['start']),
                open_price=Decimal(str(kline_data['open'])),
                high_price=Decimal(str(kline_data['high'])),
                low_price=Decimal(str(kline_data['low'])),
                close_price=Decimal(str(kline_data['close'])),
                volume=Decimal(str(kline_data['volume'])),
                turnover=Decimal(str(kline_data['turnover'])),
                symbol=symbol,
                timeframe=self._base_timeframe
            )

            await self._kline_repository.insert_batch(
                symbol,
                self._base_timeframe,
                [kline.to_tuple()]
            )

            await self._refresh_continuous_aggregate(
                self._base_timeframe,
                kline.timestamp,
                kline.timestamp + self._base_timeframe.to_milliseconds()
            )

            await self._publish_kline_update(symbol, kline)

        except Exception as e:
            logger.error(f"Error handling kline update for {symbol}: {e}")

    async def _start_streaming(self, symbol: SymbolInfo) -> None:
        """Start real-time data streaming for a symbol"""
        try:
            # Get latest timestamp to verify we have the most recent data
            latest = await self._kline_repository.get_latest_timestamp(symbol)

            if not latest:
                logger.warning(f"No historical data found for {symbol}, skipping streaming")
                return

            # Calculate the last completed interval
            current_time = TimeUtils.get_current_timestamp()
            interval_ms = self._base_timeframe.to_milliseconds()
            last_complete = current_time - (current_time % interval_ms) - interval_ms

            # Only start streaming if we have data up to the last complete interval
            if latest < last_complete:
                logger.warning(
                    f"Historical data not up to date for {symbol} "
                    f"(latest: {TimeUtils.from_timestamp(latest)}, "
                    f"expected: {TimeUtils.from_timestamp(last_complete)})"
                )
                return

            # Start streaming if not already streaming
            if symbol not in self._streaming_symbols:
                adapter = self._adapter_registry.get_adapter(symbol.exchange)
                await adapter.subscribe_klines(
                    symbol,
                    self._base_timeframe,
                    lambda data: self._handle_kline_update(symbol, data)
                )
                self._streaming_symbols.add(symbol)
                logger.info(f"Started streaming for {symbol}")

        except Exception as e:
            logger.error(f"Failed to start streaming for {symbol}: {e}")

    async def _stop_streaming(self, symbol: SymbolInfo) -> None:
        """Pause streaming for a symbol during collection"""
        if symbol in self._streaming_symbols:
            adapter = self._adapter_registry.get_adapter(symbol.exchange)
            await adapter.unsubscribe_klines(symbol, self._base_timeframe)
            self._streaming_symbols.remove(symbol)
            logger.info(f"Stopped streaming for {symbol} during collection")

    async def _refresh_continuous_aggregate(self, timeframe: Timeframe, start_time: int, end_time: int) -> None:
        """
        Refresh TimescaleDB continuous aggregate for given period.

        Args:
            kline_repository: Repository for kline operations
            timeframe: Target timeframe to refresh
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
        """
        if not timeframe.is_stored_timeframe():
            logger.debug(f"Skipping refresh for non-stored timeframe: {timeframe.value}")
            return

        # Align to timeframe boundaries
        interval_ms = timeframe.to_milliseconds()
        aligned_start = start_time - (start_time % interval_ms)
        aligned_end = end_time - (end_time % interval_ms) + interval_ms

        # Only proceed if we have valid timestamps
        if aligned_start is not None and aligned_end is not None:
            start_dt = TimeUtils.from_timestamp(aligned_start)
            end_dt = TimeUtils.from_timestamp(aligned_end)

            await self._kline_repository.refresh_continuous_aggregate(
                timeframe,
                start_dt,
                end_dt
            )
        else:
            logger.warning("Skipping continuous aggregate refresh due to invalid timestamps")

    async def cleanup(self) -> None:
        """Cleanup resources"""
        logger.info("Cleaning up DataCollector")

        # Stop all streaming
        for symbol in list(self._streaming_symbols):
            await self._stop_streaming(symbol)

        self._streaming_symbols.clear()
        self._processing_symbols.clear()
        logger.info("DataCollector cleanup completed")

    def get_collection_status(self) -> str:
        """Get collection status"""
        return "\n".join([
            "Collection Status:",
            f"Active Collections: {len(self._processing_symbols)}",
            f"Streaming Symbols: {len(self._streaming_symbols)}"
        ])
