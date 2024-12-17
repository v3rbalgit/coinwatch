import asyncio
from typing import Any, AsyncGenerator, Callable, Coroutine, Dict, Optional, Set, Tuple
from decimal import Decimal

from shared.clients.registry import ExchangeAdapterRegistry
from shared.core.enums import Timeframe
from shared.core.exceptions import ServiceError
from shared.core.models import KlineData, SymbolInfo
from shared.database.repositories.kline import KlineRepository
from shared.database.repositories.symbol import SymbolRepository
from shared.messaging.broker import MessageBroker
from shared.messaging.schemas import MessageType, KlineMessage, ErrorMessage, CollectionMessage
from shared.utils.logger import LoggerSetup
from shared.utils.progress import MarketDataProgress
from shared.utils.time import TimeUtils

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
                 base_timeframe: Timeframe = Timeframe.MINUTE_5):
        # Core dependencies
        self._adapter_registry = adapter_registry
        self._symbol_repository = symbol_repository
        self._kline_repository = kline_repository
        self._message_broker = message_broker
        self._base_timeframe = base_timeframe

        # Adapters
        self._collection_adapter = None
        self._streaming_adapter = None

        # State management - separate locks for collection and streaming
        self._collection_lock = asyncio.Lock()
        self._streaming_lock = asyncio.Lock()
        self._collection_progress: Dict[SymbolInfo, MarketDataProgress] = {}
        self._processing_symbols: Set[SymbolInfo] = set()
        self._streaming_symbols: Set[SymbolInfo] = set()


    async def collect(self, symbol: SymbolInfo, start_time: int, end_time: int, context: Dict[str, Any]) -> None:
        """
        Start price data collection for a symbol

        Args:
            symbol (SymbolInfo): Symbol to start the price data collection for.
            start_time (int): Start timestamp for collection.
            end_time (int): End timestamp for collection.
            context (Dict[str, Any]): Additional context for collection (initial, gap_fill).
        """
        try:
            # Add symbol to processing set if not already processing
            async with self._collection_lock:
                if symbol not in self._processing_symbols:
                    self._processing_symbols.add(symbol)
                    logger.info(f"Collection started for {symbol.name} on {symbol.exchange}")
                else:
                    logger.info(f"Collection already in progress for {symbol.name} on {symbol.exchange}, skipping")
                    return

            # Align the start and end timestamps to interval boundaries
            start_time, end_time = self._align_time_range((start_time, end_time), self._base_timeframe)
            if start_time == end_time:
                logger.info(f"No data to collect for {symbol.name} on {symbol.exchange}, skipping collection")
            else:
                # Collect all required data including any gaps
                await self._collect_with_gaps(symbol, start_time, end_time, context)

            # Start streaming if not already streaming
            if symbol not in self._streaming_symbols:
                await self._start_streaming(symbol)

        except Exception as e:
            await self._publish_error("CollectionError", str(e), symbol, start_time, end_time, context)
            raise
        finally:
            # Clean up collection state
            async with self._collection_lock:
                self._processing_symbols.remove(symbol)
                self._collection_progress.pop(symbol, None)

    async def _collect_with_gaps(self, symbol: SymbolInfo, start_time: int, end_time: int, context: Dict[str, Any]) -> None:
        """
        Collect data for a time range and handle any gaps that are found.
        This method will continue collecting until all gaps are filled.

        Args:
            symbol (SymbolInfo): Symbol to start the price data collection for.
            start_time (int): Start timestamp for collection.
            end_time (int): End timestamp for collection.
            context (Dict[str, Any]): Additional context for collection (initial, gap_fill).
        """
        while True:
            # Initialize progress tracking with total candles calculation
            progress = MarketDataProgress(
                symbol=symbol,
                time_range=(start_time, end_time),
                timeframe=self._base_timeframe
            )
            self._collection_progress[symbol] = progress

            # Process current range
            processed_count = 0
            async for processed in self._process_collection(symbol, start_time, end_time):
                # Update progress
                processed_count += processed
                if progress := self._collection_progress.get(symbol):
                    progress.update(processed_count)
                    logger.info(progress)

            # Log completion of current range
            if progress := self._collection_progress.get(symbol):
                logger.info(progress.get_completion_summary(TimeUtils.get_current_datetime()))

            # Publish completion message
            await self._publish_collection_complete(symbol, start_time, end_time, context)

            # Check for any gaps
            time_range = await self._verify_collection(symbol)
            if not time_range:
                break
            else:
                # Process the gap in the next iteration
                start_time, end_time = time_range
                context = {
                    "type": "gap_fill",
                    "gap_size": (end_time - start_time) // self._base_timeframe.to_milliseconds()
                }

    async def _process_collection(self, symbol: SymbolInfo, start_time: int, end_time: int) -> AsyncGenerator[int, None]:
        """
        Core collection logic for both historical and gap filling

        Args:
            symbol (SymbolInfo): Symbol to process collection for.
            start_time (int): Aligned start timestamp.
            end_time (int): Aligned end timestamp.

        Yields:
            int: Number of processed candles.
        """
        # Get collection adapter
        if not self._collection_adapter:
            self._collection_adapter = self._adapter_registry.get_adapter(symbol.exchange)

        # Process using generator pattern
        async for klines in self._collection_adapter.get_klines(
            symbol=symbol,
            timeframe=self._base_timeframe,
            start_time=start_time,
            end_time=end_time
        ):
            if not klines:
                break

            # Store batch
            processed = await self._kline_repository.insert_batch(
                symbol,
                self._base_timeframe,
                [k.to_tuple() for k in klines]
            )

            yield processed

    async def _verify_collection(self, symbol: SymbolInfo) -> Optional[Tuple[int,int]]:
        """
        Check if historical data for symbol are up-to-date

        Args:
            symbol (SymbolInfo): Symbol to check.

        Returns:
            Optional[Tuple[int,int]]: Range of timestamps if there is a gap, otherwise None.

        """
        # Get latest timestamp to verify we have the most recent data
        latest = await self._kline_repository.get_latest_timestamp(symbol)

        if not latest:
            raise ServiceError(f'No historical data found for {symbol}')

        # Calculate the last completed interval
        current_time = TimeUtils.get_current_timestamp()
        interval_ms = self._base_timeframe.to_milliseconds()
        last_complete = current_time - (current_time % interval_ms) - interval_ms

        # Check for gaps and fill if needed
        if latest < last_complete:
            logger.warning(
                f"Missing historical data for {symbol.name} on {symbol.exchange} "
                f"from {TimeUtils.from_timestamp(latest)} "
                f"to {TimeUtils.from_timestamp(last_complete)}"
            )
            return latest, last_complete

    def _align_time_range(self,time_range: Tuple[int,int], timeframe: Timeframe) -> Tuple[int, int]:
        """
        Align start and end timestamps to interval

        Args:
            time_range (Tuple[int,int]): Tuple of start and end timestamps to align.
            timeframe (Timeframe): Timeframe to base the calculation on.

        Returns:
            Tuple[int,int]: Aligned range of timestamps.
        """
        start_time, end_time = time_range
        interval_ms = timeframe.to_milliseconds()
        aligned_start = start_time - (start_time % interval_ms)
        current_time = TimeUtils.get_current_timestamp()
        aligned_end = min(
            end_time - (end_time % interval_ms),
            current_time - (current_time % interval_ms) - interval_ms
        )
        return aligned_start, aligned_end

    async def _start_streaming(self, symbol: SymbolInfo) -> None:
        """Start real-time data streaming for a symbol"""
        try:
            async with self._streaming_lock:
                if not self._streaming_adapter:
                    self._streaming_adapter = self._adapter_registry.get_adapter(symbol.exchange)

                # Start streaming if not already streaming
                if symbol not in self._streaming_symbols:
                    # Create handler for this symbol
                    handler = await self._create_kline_handler(symbol)

                    # Subscribe to klines
                    await self._streaming_adapter.subscribe_klines(symbol, self._base_timeframe, handler)

                    self._streaming_symbols.add(symbol)
                    logger.info(f"Streaming started for {symbol.name} on {symbol.exchange}")

        except Exception as e:
            logger.error(f"Failed to start streaming for {symbol}: {e}")
            await self._publish_error("StreamingError", str(e), symbol, 0, 0, {"type": "streaming"})
            raise

    async def _stop_streaming(self, symbol: SymbolInfo) -> None:
        """Stop streaming for a symbol"""
        async with self._streaming_lock:
            if symbol in self._streaming_symbols:
                try:
                    if not self._streaming_adapter:
                        self._streaming_adapter = self._adapter_registry.get_adapter(symbol.exchange)

                    # Unsubscribe from websocket
                    await self._streaming_adapter.unsubscribe_klines(symbol, self._base_timeframe)

                    self._streaming_symbols.remove(symbol)
                    logger.info(f"Streaming stopped for {symbol.name} on {symbol.exchange}")

                except Exception as e:
                    logger.error(f"Error stopping streaming for {symbol}: {e}")
                    raise

    async def _create_kline_handler(self, symbol: SymbolInfo) -> Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]:
        """Create a handler for kline updates"""
        async def handle_update(data: Dict[str, Any]) -> None:
            try:
                # Extract kline data from the message
                kline_data = data.get('data', [{}])[0]

                # Skip unconfirmed candles
                if not kline_data.get('confirm', False):
                    return

                # Create KlineData object
                kline = KlineData(
                    symbol=symbol,
                    timeframe=self._base_timeframe,
                    timestamp=int(kline_data['start']),
                    open_price=Decimal(str(kline_data['open'])),
                    high_price=Decimal(str(kline_data['high'])),
                    low_price=Decimal(str(kline_data['low'])),
                    close_price=Decimal(str(kline_data['close'])),
                    volume=Decimal(str(kline_data['volume'])),
                    turnover=Decimal(str(kline_data['turnover']))
                )

                # Store the kline
                await self._kline_repository.insert_batch(symbol, self._base_timeframe, [kline.to_tuple()])

                # Publish update
                await self._publish_kline_update(symbol, kline)

                logger.info(f"New kline for {symbol.name} on {symbol.exchange}")

            except Exception as e:
                logger.error(f"Error handling kline update for {symbol}: {e}")
                await self._publish_error("StreamingError", str(e), symbol, 0, 0, {"type": "streaming_update"})

        return handle_update

    async def delist(self, symbol: SymbolInfo) -> None:
        """Handle symbol delisting"""
        try:
            await self._stop_streaming(symbol)
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

    async def _publish_collection_complete(self, symbol: SymbolInfo, start_time: int, end_time: int, context: Dict[str, Any]) -> None:
        """Publish collection complete message"""
        await self._message_broker.publish(
            MessageType.COLLECTION_COMPLETE,
            CollectionMessage(
                service="market_data",
                type=MessageType.COLLECTION_COMPLETE,
                timestamp=TimeUtils.get_current_timestamp(),
                timeframe=self._base_timeframe.value,
                symbol=symbol.name,
                exchange=symbol.exchange,
                start_time=start_time,
                end_time=end_time,
                processed=self._collection_progress[symbol].processed_candles,
                context=context
            ).model_dump()
        )

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
                timeframe=self._base_timeframe.value,
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
                    "timeframe": self._base_timeframe.value
                }
            ).model_dump()
        )

    async def cleanup(self) -> None:
        """Cleanup resources"""
        logger.info("Cleaning up DataCollector")

        try:
            # Stop all streaming
            streaming_symbols = list(self._streaming_symbols)
            for symbol in streaming_symbols:
                await self._stop_streaming(symbol)

            # Cleanup streaming adapter if it exists
            if self._streaming_adapter:
                await self._streaming_adapter.cleanup()

            self._streaming_symbols.clear()
            self._processing_symbols.clear()
            logger.info("DataCollector cleanup completed")

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            raise

    def get_collection_status(self) -> str:
        """Get collection status"""
        status_lines = [
            "Collection Status:",
            f"Active Collections: {len(self._processing_symbols)}",
            f"Streaming Symbols: {len(self._streaming_symbols)}"
        ]

        # Add details about active collections
        if self._processing_symbols:
            status_lines.append("\nActive Collections:")
            for symbol in self._processing_symbols:
                if progress := self._collection_progress.get(symbol):
                    percentage = progress.get_percentage()
                    status_lines.append(
                        f"  {symbol}: {percentage:.1f}% complete "
                        f"({progress.processed_candles}/{progress.total_candles} candles)"
                    )

        return "\n".join(status_lines)
