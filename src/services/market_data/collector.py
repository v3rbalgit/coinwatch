# src/services/market_data/collector.py

import asyncio
import random
from typing import Optional, Set

from .symbol_state import SymbolState, SymbolStateManager
from ...adapters.registry import ExchangeAdapterRegistry
from ...core.coordination import Command, MarketDataCommand, ServiceCoordinator
from ...repositories.kline import KlineRepository
from ...repositories.symbol import SymbolRepository
from ...utils.logger import LoggerSetup
from ...utils.time import from_timestamp, get_current_timestamp
from ...utils.domain_types import Timeframe, Timestamp
from ...core.models import SymbolInfo

logger = LoggerSetup.setup(__name__)

class HistoricalCollector:
    """Handles historical data collection for symbols"""

    def __init__(self,
                 adapter_registry: ExchangeAdapterRegistry,
                 symbol_repository: SymbolRepository,
                 kline_repository: KlineRepository,
                 state_manager: SymbolStateManager,
                 coordinator: ServiceCoordinator,
                 max_retries: int = 3,
                 batch_size: int = 1000):

        self._collection_queue: asyncio.Queue[SymbolInfo] = asyncio.Queue(maxsize=1000)
        self._adapter_registry = adapter_registry
        self._symbol_repository = symbol_repository
        self._kline_repository = kline_repository
        self._coordinator = coordinator
        self._state_manager = state_manager
        self._max_retries = max_retries
        self._batch_size = batch_size

        self._active = False
        self._task: Optional[asyncio.Task] = None

        # Add synchronization primitives
        self._processing_symbols: Set[SymbolInfo] = set()
        self._collection_lock = asyncio.Lock()

    async def _register_command_handlers(self) -> None:
        """Register command handlers"""
        await self._coordinator.register_handler(
            MarketDataCommand.ADJUST_BATCH_SIZE,
            self._handle_batch_size_adjustment
        )
        await self._coordinator.register_handler(
            MarketDataCommand.PAUSE_COLLECTION,
            self._handle_pause_collection
        )
        await self._coordinator.register_handler(
            MarketDataCommand.RESUME_COLLECTION,
            self._handle_resume_collection
        )

    async def _handle_batch_size_adjustment(self, command: Command) -> None:
        """Handle batch size adjustment command"""
        new_size = command.params.get('size')
        if new_size:
            self._batch_size = max(100, min(new_size, 1000))
            logger.info(f"Adjusted historical collection batch size to {self._batch_size}")

    async def _handle_pause_collection(self, command: Command) -> None:
        """Handle collection pause command"""
        symbol_name = command.params.get('symbol')
        if symbol_name:
            # Get symbol info from state manager
            for symbol in self._processing_symbols:
                if symbol.name == symbol_name:
                    self._processing_symbols.remove(symbol)
                    logger.info(f"Paused historical collection for {symbol_name}")

                    # Update state to reflect pause
                    await self._state_manager.update_state(
                        symbol,
                        SymbolState.HISTORICAL,
                        "Collection paused by command"
                    )
                    break

    async def _handle_resume_collection(self, command: Command) -> None:
        """Handle collection resume command"""
        symbol_name = command.params.get('symbol')
        if symbol_name:
            # Find symbol in state manager
            symbols = await self._state_manager.get_symbols_by_state(SymbolState.HISTORICAL)
            for symbol in symbols:
                if symbol.name == symbol_name:
                    await self.add_symbol(symbol)
                    logger.info(f"Resumed historical collection for {symbol_name}")
                    break

    async def start(self) -> None:
        """Start the collector"""
        self._active = True
        self._task = asyncio.create_task(self._process_queue())
        logger.info("Historical collector started")

    async def stop(self) -> None:
        """Stop collector with proper cleanup"""
        self._active = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        # Clean up any pending items
        while not self._collection_queue.empty():
            symbol = await self._collection_queue.get()
            async with self._collection_lock:
                self._processing_symbols.remove(symbol)
            self._collection_queue.task_done()
        logger.info("Historical collector stopped")

    async def add_symbol(self, symbol: SymbolInfo) -> None:
        """Add symbol for historical collection"""
        async with self._collection_lock:
            if symbol not in self._processing_symbols:
                await self._state_manager.add_symbol(symbol)
                self._processing_symbols.add(symbol)
                await self._collection_queue.put(symbol)

                # Initialize collection progress
                await self._state_manager.update_collection_progress(
                    symbol,
                    processed=0,
                    total=None  # Will be calculated when collection starts
                )

                # Notify about new collection
                await self._coordinator.execute(Command(
                    type=MarketDataCommand.COLLECTION_STARTED,
                    params={
                        "symbol": symbol.name,
                        "start_time": symbol.launch_time
                    }
                ))

                logger.info(f"Added {symbol.name} to historical collection queue")
            else:
                logger.debug(f"Symbol {symbol.name} already in processing queue")

    async def _process_queue(self) -> None:
        """Process symbols in the queue with proper state management"""
        while self._active:
            try:
                symbol = await self._collection_queue.get()

                try:
                    # Update state to HISTORICAL before processing
                    await self._state_manager.update_state(
                        symbol,
                        SymbolState.HISTORICAL
                    )

                    await self._collect_historical_data(symbol, Timeframe.MINUTE_5)
                    await self._state_manager.update_state(
                        symbol,
                        SymbolState.READY
                    )
                    logger.info(f"Syncing {symbol.name} ({symbol.exchange})...")
                except Exception as e:
                    await self._state_manager.update_state(
                        symbol,
                        SymbolState.ERROR,
                        str(e)
                    )
                finally:
                    # Remove from processing set
                    async with self._collection_lock:
                        self._processing_symbols.remove(symbol)
                    self._collection_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in collection queue processing: {e}")
                await asyncio.sleep(1)

    async def _collect_historical_data(self, symbol: SymbolInfo, timeframe: Timeframe) -> None:
        """Collect historical data using centralized progress tracking"""
        adapter = self._adapter_registry.get_adapter(symbol.exchange)
        retry_count = 0

        while retry_count < self._max_retries:
            try:
                # First ensure symbol exists in database
                await self._symbol_repository.get_or_create(symbol)

                # Get start time based on launch or last data
                start_time = await self._kline_repository.get_latest_timestamp(
                    symbol,
                    timeframe
                )

                current_time = get_current_timestamp()
                interval_ms = timeframe.to_milliseconds()

                # Adjust to last completed interval
                last_complete_interval = current_time - (current_time % interval_ms) - interval_ms

                if not start_time:
                    start_time = symbol.launch_time
                else:
                    start_time += interval_ms

                # Calculate total expected candles
                total_candles = int((last_complete_interval - start_time) // interval_ms)
                processed_candles = 0

                # Update initial progress
                await self._state_manager.update_collection_progress(
                    symbol,
                    processed=processed_candles,
                    total=total_candles
                )

                logger.info(
                    f"Starting historical collection for {symbol.name} "
                    f"from {from_timestamp(start_time)} "
                    f"to {from_timestamp(last_complete_interval)}"
                )

                while start_time < last_complete_interval:
                    # Check if collection was paused
                    if symbol not in self._processing_symbols:
                        logger.info(f"Collection paused for {symbol.name}")
                        return

                    # Fetch batch of historical data
                    klines = await adapter.get_klines(
                        symbol=symbol,
                        timeframe=timeframe,
                        start_time=Timestamp(start_time),
                        limit=self._batch_size
                    )

                    if not klines:
                        break

                    # Only store completed candles
                    klines = [k for k in klines if k.timestamp <= last_complete_interval]

                    if klines:
                        # Store klines
                        await self._kline_repository.insert_batch(
                            symbol,
                            timeframe,
                            [k.to_tuple() for k in klines]
                        )

                        # Update progress
                        last_timestamp = klines[-1].timestamp
                        processed_candles += len(klines)

                        await self._state_manager.update_collection_progress(
                            symbol,
                            processed=processed_candles,
                            total=total_candles
                        )

                        await self._state_manager.update_sync_time(
                            symbol,
                            Timestamp(last_timestamp)
                        )

                        # Notify about progress
                        percentage = min(100, (processed_candles / total_candles) * 100)
                        await self._coordinator.execute(Command(
                            type=MarketDataCommand.COLLECTION_PROGRESS,
                            params={
                                "symbol": symbol.name,
                                "progress": percentage,
                                "processed": processed_candles,
                                "total": total_candles
                            }
                        ))

                        start_time = last_timestamp + interval_ms

                logger.info(
                    f"Completed historical collection for {symbol.name} "
                    f"up to {from_timestamp(last_complete_interval)}"
                )

                # Collection complete
                await self._coordinator.execute(Command(
                    type=MarketDataCommand.COLLECTION_COMPLETE,
                    params={
                        "symbol": symbol.name,
                        "processed_candles": processed_candles,
                        "start_time": symbol.launch_time,
                        "end_time": last_complete_interval
                    }
                ))
                break

            except Exception as e:
                retry_count += 1

                # Update error in state manager
                await self._state_manager.update_collection_progress(
                    symbol,
                    processed=processed_candles,
                    total=total_candles,
                    error=str(e)
                )

                if retry_count >= self._max_retries:
                    raise

                delay = min(30, 2 ** (retry_count - 1))
                delay = delay * (0.5 + random.random())  # Add jitter

                logger.warning(
                    f"Retry {retry_count}/{self._max_retries} "
                    f"for {symbol.name} after {delay:.2f}s: {e}"
                )

                await asyncio.sleep(delay)

    def get_collection_status(self) -> str:
        """Get collection status using state manager"""
        status = ["Historical Collection Status:"]

        active_collections = len(self._processing_symbols)
        pending_collections = self._collection_queue.qsize()

        status.extend([
            f"Active Collections: {active_collections}",
            f"Pending Collections: {pending_collections}"
        ])

        return "\n".join(status)