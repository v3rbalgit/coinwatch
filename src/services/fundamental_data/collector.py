# src/services/fundamental_data/collector.py

from abc import ABC, abstractmethod
from typing import Dict, Set, Any
import asyncio

from ...core.models import SymbolInfo
from ...utils.retry import RetryConfig, RetryStrategy
from ...utils.domain_types import Timestamp
from ...utils.time import TimeUtils
from ...utils.logger import LoggerSetup
from ...utils.progress import FundamentalDataProgress

logger = LoggerSetup.setup(__name__)


class FundamentalCollector(ABC):
    """
    Abstract base class for fundamental data collectors.

    Implements common functionality for collecting and managing fundamental data
    for different types of metrics (metadata, market, blockchain, sentiment).
    """

    def __init__(self,
                 collection_interval: int,
                 batch_size: int):
        """
        Initialize the FundamentalCollector.

        Args:
            collection_interval (int): Time interval between collections in seconds.
            batch_size (int): Number of symbols to process concurrently.
        """
        self._collection_interval = collection_interval
        self._batch_size = batch_size

        # Collection management
        self._collection_queue: asyncio.Queue[SymbolInfo] = asyncio.Queue()
        self._collection_lock = asyncio.Lock()
        self._processing: Set[SymbolInfo] = set()
        self._progress: Dict[SymbolInfo, FundamentalDataProgress] = {}
        self._last_collection: Dict[SymbolInfo, Timestamp] = {}

        # Initialize retry strategy
        self._retry_strategy = RetryStrategy(RetryConfig(
            base_delay=1.0,
            max_delay=30.0,
            max_retries=3,
            jitter_factor=0.1
        ))

        # Start collection worker
        self._queue_processor = asyncio.create_task(self._collection_worker())

    @property
    @abstractmethod
    def collector_type(self) -> str:
        """
        Return the type of collector (e.g., 'metadata', 'market', etc.).

        Returns:
            str: The collector type.
        """
        pass

    @abstractmethod
    async def collect_symbol_data(self, symbol: SymbolInfo) -> Dict[str, Any]:
        """
        Collect fundamental data for a specific symbol.

        Args:
            symbol (SymbolInfo): Symbol to collect data for.

        Returns:
            Dict[str, Any]: Collected metric data.
        """
        pass

    @abstractmethod
    async def store_symbol_data(self, symbol: SymbolInfo, data: Dict[str, Any]) -> None:
        """
        Store collected data for a symbol.

        Args:
            symbol (SymbolInfo): Symbol the data belongs to.
            data (Dict[str, Any]): Collected metric data.
        """
        pass

    async def cleanup(self) -> None:
        """Clean up resources and cancel ongoing tasks."""
        if self._queue_processor:
            self._queue_processor.cancel()
            try:
                await self._queue_processor
            except asyncio.CancelledError:
                pass

        # Clear collections
        async with self._collection_lock:
            self._processing.clear()
            self._progress.clear()
            self._last_collection.clear()

        while not self._collection_queue.empty():
            try:
                self._collection_queue.get_nowait()
                self._collection_queue.task_done()
            except asyncio.QueueEmpty:
                break

    async def schedule_collection(self, symbol: SymbolInfo) -> None:
        """
        Schedule data collection for a symbol if enough time has passed.

        Args:
            symbol (SymbolInfo): Symbol to schedule for collection.
        """
        current_time = TimeUtils.get_current_timestamp()
        last_collection = self._last_collection.get(symbol)

        if (not last_collection or
            current_time - last_collection >= self._collection_interval * 1000):
            if symbol not in self._processing:
                await self._collection_queue.put(symbol)

    async def _collection_worker(self) -> None:
        """Main collection loop processing symbols from the queue."""
        while True:
            try:
                async with asyncio.Semaphore(self._batch_size):
                    symbol = await self._collection_queue.get()
                    try:
                        await self._process_symbol(symbol)
                    except Exception as e:
                        logger.error(f"Error collecting {symbol}: {e}")
                    finally:
                        self._collection_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker error: {e}")
                await asyncio.sleep(1)

    async def _process_symbol(self, symbol: SymbolInfo) -> None:
        """
        Process a single symbol by collecting and storing its data.

        Args:
            symbol (SymbolInfo): Symbol to process.
        """
        async with self._collection_lock:
            if symbol in self._processing:
                return
            self._processing.add(symbol)

        try:
            progress = FundamentalDataProgress(
                symbol=symbol,
                collector_type=self.collector_type,
                start_time=TimeUtils.get_current_datetime()
            )
            self._progress[symbol] = progress

            # Collect and store data
            data = await self.collect_symbol_data(symbol)
            await self.store_symbol_data(symbol, data)

            # Update progress and collection time
            progress.status = "completed"
            self._last_collection[symbol] = TimeUtils.get_current_timestamp()

            logger.info(progress.get_completion_summary(TimeUtils.get_current_datetime()))

        except Exception as e:
            if progress := self._progress.get(symbol):
                progress.status = "error"
                progress.error = str(e)
            logger.error(f"Error processing {symbol}: {e}")
            raise

        finally:
            async with self._collection_lock:
                self._processing.remove(symbol)
                self._progress.pop(symbol, None)

    def get_collection_status(self) -> str:
        """
        Get a summary of the collector's current status.

        Returns:
            str: A formatted string with collection status information.
        """
        return (
            f"Active Collections: {len(self._processing)}\n"
            f"Queued Collections: {self._collection_queue.qsize()}\n"
            f"Last Collection Times:\n" +
            "\n".join(
                f"  {symbol}: {TimeUtils.from_timestamp(timestamp)}"
                for symbol, timestamp in self._last_collection.items()
            )
        )