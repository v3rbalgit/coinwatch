import asyncio
from typing import Any, AsyncGenerator, Callable, Coroutine
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from websockets.exceptions import ConnectionClosedError

from shared.clients.registry import ExchangeAdapterRegistry
from shared.core.enums import Interval
from shared.core.models import KlineModel, SymbolModel
from shared.database.repositories.kline import KlineRepository
from shared.database.repositories.symbol import SymbolRepository
from shared.utils.logger import LoggerSetup
from shared.utils.progress import MarketDataProgress
from shared.utils.time import align_time_range, get_current_timestamp, format_timestamp



class KlineCollector:
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
                 base_interval: Interval = Interval.MINUTE_5):
        # Core dependencies
        self._adapter_registry = adapter_registry
        self._symbol_repository = symbol_repository
        self._kline_repository = kline_repository
        self._base_interval = base_interval

        # Adapters
        self._collection_adapter = None
        self._streaming_adapter = None

        # State management - separate locks for collection and streaming
        self._collection_lock = asyncio.Lock()
        self._collection_symbols: set[SymbolModel] = set()
        self._collection_progress: dict[SymbolModel, MarketDataProgress] = {}
        self._streaming_lock = asyncio.Lock()
        self._streaming_symbols: set[SymbolModel] = set()

        self.logger = LoggerSetup.setup(__class__.__name__)


    async def collect(self, symbol: SymbolModel, start_time: int, end_time: int) -> None:
        """
        Start price data collection for a symbol

        Args:
            symbol (SymbolModel): Symbol to start the price data collection for.
            start_time (int): Start timestamp for collection.
            end_time (int): End timestamp for collection.
            context (Dict[str, Any]): Additional context for collection (initial, gap_fill).
        """
        try:
            # Add symbol to processing set if not already processing
            async with self._collection_lock:
                if symbol not in self._collection_symbols:
                    self._collection_symbols.add(symbol)
                    self.logger.info(f"Collection started for {str(symbol)}")
                else:
                    self.logger.info(f"Collection already in progress for {str(symbol)}, skipping...")
                    return

            # Align the start and end timestamps to interval boundaries
            start_time, end_time = align_time_range(start_time, end_time, self._base_interval)
            if start_time == end_time:
                self.logger.info(f"No data to collect for {str(symbol)}, skipping...")
            else:
                # Collect all required data including any gaps
                await self._collect_with_gaps(symbol, start_time, end_time)

            # Start streaming if not already streaming
            if symbol not in self._streaming_symbols:
                await self._start_streaming(symbol)

        finally:
            # Clean up collection state
            async with self._collection_lock:
                self._collection_symbols.remove(symbol)
                self._collection_progress.pop(symbol, None)


    async def _collect_with_gaps(self, symbol: SymbolModel, start_time: int, end_time: int) -> None:
        """
        Collect data for a time range and handle any gaps that are found.
        This method will continue collecting until all gaps are filled.

        Args:
            symbol (SymbolModel): Symbol to start the price data collection for.
            start_time (int): Start timestamp for collection.
            end_time (int): End timestamp for collection.
        """
        while True:
            # Initialize progress tracking with actual trading start time
            self._collection_progress[symbol] = MarketDataProgress(
                symbol=symbol,
                time_range=(start_time, end_time),
                interval=self._base_interval
            )

            # Process current range
            async for processed, gap in self._process_collection(symbol, start_time, end_time):
                if progress := self._collection_progress.get(symbol):
                    if gap > 0:
                        self.logger.warning(f"Start time gap found for {str(symbol)}: {gap} klines, adjusting...")
                    progress.update(processed, gap)
                    self.logger.info(progress)
                # Add a small delay between batches to let the streams come through
                await asyncio.sleep(0.01)

            # Log completion of current range
            if progress := self._collection_progress.get(symbol):
                self.logger.info(progress.get_completion_summary())

            # Check for any gaps
            time_range = await self._verify_collection(symbol, start_time, end_time)

            if not time_range:
                break

            # Process the gap in the next iteration
            start_time, end_time = time_range

            self.logger.warning(
                f"Missing historical data for {str(symbol)} "
                f"from {format_timestamp(start_time)} "
                f"to {format_timestamp(end_time)}, backfilling..."
            )


    async def _process_collection(self, symbol: SymbolModel, start_time: int, end_time: int) -> AsyncGenerator[tuple[int, int], None]:
        """
        Core collection logic for both fetching and upserting kline data

        Args:
            symbol (SymbolModel): Symbol to process collection for.
            start_time (int): Aligned start timestamp.
            end_time (int): Aligned end timestamp.

        Yields:
            Tuple[int, int]: Tuple of (processed, gap) for each batch.
                If there is a gap, it means that the start_time was different
                than actual timestamp of the first candle and needs to be adjusted.
        """
        # Get collection adapter
        if not self._collection_adapter:
            self._collection_adapter = self._adapter_registry.get_adapter(symbol.exchange)

        # Process using generator pattern
        async for klines, gap in self._collection_adapter.get_klines(
            symbol=symbol,
            interval=self._base_interval,
            start_time=start_time,
            end_time=end_time
        ):
            if not klines:
                break

            # Store batch
            processed = await self._kline_repository.upsert_klines(symbol, klines)

            yield processed, gap


    async def _verify_collection(self, symbol: SymbolModel, start_time: int, end_time: int) -> tuple[int,int] | None:
        """
        Check if historical data for symbol are up-to-date and return first gap found up until latest complete candle's timestamp

        Args:
            symbol (SymbolModel): Symbol to check.
            start_time (int): Start timestamp to check from
            end_time (int): Original end timestamp (used as minimum boundary)

        Returns:
            Optional[Tuple[int,int]]: Range of timestamps if there is a gap, otherwise None.
        """
        # Calculate the last completed interval - this is our actual end time for gap checking
        current_time = get_current_timestamp()
        interval_ms = self._base_interval.to_milliseconds()
        last_complete = current_time - (current_time % interval_ms) - interval_ms

        # Use max of original end_time and last_complete to ensure we catch any new candles
        actual_end = max(end_time, last_complete)

        gaps = await self._kline_repository.get_data_gaps(symbol, self._base_interval, start_time, actual_end)

        # Process first gap found if it ends before the last complete interval
        if gaps and gaps[0][1] < last_complete:
            return gaps[0]

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=1, max=60),
        retry=retry_if_exception_type(ConnectionClosedError),
        reraise=True
    )
    async def _start_streaming(self, symbol: SymbolModel) -> None:
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
                    await self._streaming_adapter.subscribe_klines(symbol, self._base_interval, handler)

                    self._streaming_symbols.add(symbol)
                    self.logger.info(f"Kline streaming started for {str(symbol)}")

        except ConnectionClosedError:
            # Let the retry mechanism handle it
            raise
        except Exception as e:
            # Log and raise other exceptions immediately
            self.logger.error(
                f"Failed to start streaming klines for {str(symbol)}: {e}\n"
                f"Exception type: {type(e).__name__}\n"
            )
            raise


    async def _stop_streaming(self, symbol: SymbolModel) -> None:
        """Stop streaming for a symbol"""
        async with self._streaming_lock:
            if symbol in self._streaming_symbols:
                try:
                    if not self._streaming_adapter:
                        self._streaming_adapter = self._adapter_registry.get_adapter(symbol.exchange)

                    # Unsubscribe from websocket
                    await self._streaming_adapter.unsubscribe_klines(symbol, self._base_interval)

                    self._streaming_symbols.remove(symbol)
                    self.logger.info(f"Streaming stopped for {str(symbol)}")

                except Exception as e:
                    self.logger.error(f"Error stopping streaming for {str(symbol)}: {e}")
                    raise


    async def _create_kline_handler(self, symbol: SymbolModel) -> Callable[[dict[str, Any]], Coroutine[Any, Any, None]]:
        """Create a handler for kline updates"""
        async def handle_update(data: dict[str, Any]) -> None:
            try:
                # Extract kline data from the message
                kline_data = data.get('data', [{}])[0]

                # Skip unconfirmed candles
                if not kline_data.get('confirm', False):
                    return

                kline_list = [kline_data[k] for k in ['start', 'open', 'high', 'low', 'close', 'volume', 'turnover']]

                # Create KlineModel object with proper precision
                kline = KlineModel.from_raw_data(kline_list, self._base_interval)

                # Store the kline
                await self._kline_repository.upsert_klines(symbol, [kline])

                self.logger.info(f"New complete kline for {str(symbol)}: {kline.start_time}")

            except Exception as e:
                self.logger.error(f"Error handling kline update for {str(symbol)}: {e}")
                end_time = get_current_timestamp()
                start_time = end_time - (5 * self._base_interval.to_milliseconds())     # try to get last 5 candles on streaming error
                await self.collect(
                    symbol=symbol,
                    start_time=start_time,
                    end_time=end_time
                )

        return handle_update


    async def delist(self, symbol: SymbolModel) -> None:
        """Handle symbol delisting"""
        try:
            await self._stop_streaming(symbol)
            self.logger.info(f"Stopped streaming for delisted symbol {str(symbol)}")

            # Remove from collection if in progress
            async with self._collection_lock:
                if symbol in self._collection_symbols:
                    self._collection_symbols.remove(symbol)
                    self.logger.info(f"Removed delisted symbol from collection {str(symbol)}")

            # Clean up data
            await self._symbol_repository.delete_symbol(symbol)
            await self._kline_repository.delete_symbol_data(symbol)
            self.logger.info(f"Cleaned up data for delisted symbol {str(symbol)}")

        except Exception as e:
            self.logger.error(f"Error handling symbol delisting for {str(symbol)}: {e}")


    async def cleanup(self) -> None:
        """Cleanup resources"""
        self.logger.info("Cleaning up KlineCollector")

        try:
            # Stop all streaming
            streaming_symbols = list(self._streaming_symbols)
            for symbol in streaming_symbols:
                await self._stop_streaming(symbol)

            # Cleanup streaming adapter if it exists
            if self._streaming_adapter:
                await self._streaming_adapter.cleanup()

            self._streaming_symbols.clear()
            self._collection_symbols.clear()
            self.logger.info("KlineCollector cleanup completed")

        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
            raise


    def get_collection_status(self) -> str:
        """Get collection status"""
        status_lines = [
            "Collection Status:",
            f"Active Collections: {len(self._collection_symbols)}",
            f"Streaming Symbols: {len(self._streaming_symbols)}"
        ]

        # Add details about active collections
        if self._collection_symbols:
            status_lines.append("\nActive Collections:")
            for symbol in self._collection_symbols:
                if progress := self._collection_progress.get(symbol):
                    percentage = progress.get_percentage()
                    status_lines.append(
                        f"  {symbol}: {percentage:.1f}% complete "
                        f"({progress.processed_candles}/{progress.total_candles} candles)"
                    )

        return "\n".join(status_lines)
