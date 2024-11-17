# src/services/fundamental_data/collector.py

from abc import ABC, abstractmethod
from typing import Dict, List, Set
import asyncio

from ...core.models import MarketMetrics, Metadata
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
                 collection_interval: int):
        """
        Initialize the FundamentalCollector.

        Args:
            collection_interval (int): Time interval between collections in seconds.
        """
        self._collection_interval = collection_interval

        # Collection management
        self._collection_queue: asyncio.Queue[Set[str]] = asyncio.Queue()
        self._collection_lock = asyncio.Lock()
        self._processing: Set[str] = set()
        self._progress: Dict[str, FundamentalDataProgress] = {}
        self._last_collection: Dict[str, Timestamp] = {}

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
    async def collect_symbol_data(self, tokens: List[str]) -> List[MarketMetrics | Metadata]:
        """
        Collect fundamental data for specific symbols.

        Args:
            tokens (List[str]): List of symbols to collect data for.

        Returns:
            List[MarketMetrics | Metadata]: Collected metric data for the symbols.
        """
        pass

    @abstractmethod
    async def store_symbol_data(self, data: List[MarketMetrics | Metadata]) -> None:
        """
        Store collected data for symbols.

        Args:
            data (List[MarketMetrics | Metadata]): Collected metric data to store.

        """
        pass

    @abstractmethod
    async def delete_symbol_data(self, token: str) -> None:
        """
        Delete data collection for symbol.

        Args:
            token (str): Symbol to delete.

        """
        pass

    async def schedule_collection(self, tokens: Set[str]) -> None:
        """
        Schedule data collection for a symbol if enough time has passed.

        Args:
            tokens (Set[str]): Set of symbols to schedule for collection.
        """
        current_time = TimeUtils.get_current_timestamp()

        async with self._collection_lock:
            # Filter tokens that need collection
            tokens_to_collect = {
                token for token in tokens
                if token not in self._processing and (
                    token not in self._last_collection or
                    current_time - self._last_collection[token] >= self._collection_interval * 1000
                )
            }

            if tokens_to_collect:
                await self._collection_queue.put(tokens_to_collect)

    async def _collection_worker(self) -> None:
        """
        Main collection loop processing symbols from the queue.

        This method runs continuously, processing batches of symbols from the collection queue.
        """
        while True:
            try:
                    symbols = await self._collection_queue.get()
                    try:
                        await self._process_symbols(symbols)
                    except Exception as e:
                        logger.error(f"Error collecting {symbols}: {e}")
                    finally:
                        self._collection_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker error: {e}")
                await asyncio.sleep(1)

    async def _process_symbols(self, tokens: Set[str]) -> None:
        """
        Process a set of symbols by collecting and storing their data.

        Args:
            tokens (Set[str]): Set of symbols to process.

        Raises:
            Exception: If an error occurs during processing. The error is logged and re-raised.
        """
        processed_tokens = set()

        async with self._collection_lock:
            # Update processing set atomically
            new_tokens = tokens - self._processing
            if not new_tokens:
                return
            self._processing.update(new_tokens)
            processed_tokens = new_tokens.copy()

        try:
            for token in processed_tokens:
                progress = FundamentalDataProgress(
                    symbol=token,
                    collector_type=self.collector_type,
                    start_time=TimeUtils.get_current_datetime()
                )
                self._progress[token] = progress

            # Collect and store data
            data = await self.collect_symbol_data([*processed_tokens])
            await self.store_symbol_data(data)

            # Update progress and collection time
            current_time = TimeUtils.get_current_timestamp()
            for token in processed_tokens:
                if progress := self._progress.get(token):
                    progress.status = "completed"
                self._last_collection[token] = current_time

        except Exception as e:
            for token in processed_tokens:
                if progress := self._progress.get(token):
                    progress.status = "error"
                    progress.error = str(e)
            logger.error(f"Error processing tokens {processed_tokens}: {e}")
            raise

        finally:
            async with self._collection_lock:
                self._processing.difference_update(processed_tokens)
                for token in processed_tokens:
                    self._progress.pop(token, None)

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

    def get_collection_status(self) -> str:
        """
        Get a concise summary of the collector's current status.

        Returns:
            str: A formatted string with key collection status information.
        """
        current_time = TimeUtils.get_current_timestamp()

        return (
            f"Collector: {self.collector_type}\n"
            f"Active: {len(self._processing)} | Queued: {self._collection_queue.qsize()}\n"
            f"Last collection:\n" +
            "\n".join(
                f"  {symbol}: {TimeUtils.format_time_difference(current_time - timestamp)} ago"
                for symbol, timestamp in sorted(
                    self._last_collection.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:5]  # Show only the 5 most recent collections
            )
        )