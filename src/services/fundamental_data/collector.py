# src/services/fundamental_data/collector.py

from abc import ABC, abstractmethod
from typing import Dict, Set, Any
import asyncio

from ...core.models import SymbolInfo
from ...core.coordination import ServiceCoordinator
from ...utils.retry import RetryConfig, RetryStrategy
from ...utils.domain_types import Timestamp
from ...utils.time import TimeUtils
from ...utils.logger import LoggerSetup
from ...config import MarketDataConfig
from ..market_data.progress import CollectionProgress

logger = LoggerSetup.setup(__name__)

class FundamentalCollector(ABC):
    """
    Base class for fundamental data collectors.

    Each collector type (metadata, market, blockchain, sentiment)
    will implement this interface for its specific metrics.
    """

    def __init__(self,
                 coordinator: ServiceCoordinator,
                 config: MarketDataConfig):
        self.coordinator = coordinator
        self.config = config

        # Collection management
        self._collection_queue: asyncio.Queue[SymbolInfo] = asyncio.Queue()
        self._collection_lock = asyncio.Lock()
        self._processing: Set[SymbolInfo] = set()
        self._progress: Dict[SymbolInfo, CollectionProgress] = {}

        # Status tracking
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
        """Return the type of collector (e.g., 'metadata', 'market', etc.)"""
        pass

    @abstractmethod
    async def collect_symbol_data(self, symbol: SymbolInfo) -> Dict[str, Any]:
        """
        Collect fundamental data for a specific symbol.

        Args:
            symbol: Symbol to collect data for

        Returns:
            Dict containing collected metrics
        """
        pass

    @abstractmethod
    async def store_symbol_data(self, symbol: SymbolInfo, data: Dict[str, Any]) -> None:
        """
        Store collected data for a symbol.

        Args:
            symbol: Symbol the data belongs to
            data: Collected metric data
        """
        pass

    async def cleanup(self) -> None:
        """Cleanup resources"""
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
        """Schedule data collection for a symbol"""
        if symbol not in self._processing:
            await self._collection_queue.put(symbol)

    async def _collection_worker(self) -> None:
        """Main collection loop"""
        while True:
            try:
                async with asyncio.Semaphore(self.config.batch_size):
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
        """Process a single symbol"""
        async with self._collection_lock:
            if symbol in self._processing:
                return
            self._processing.add(symbol)

        try:
            progress = CollectionProgress(
                symbol=symbol,
                start_time=TimeUtils.get_current_datetime()
            )
            self._progress[symbol] = progress

            # Collect and store data
            data = await self.collect_symbol_data(symbol)
            await self.store_symbol_data(symbol, data)

            # Update collection time
            self._last_collection[symbol] = TimeUtils.get_current_timestamp()

            if progress := self._progress.get(symbol):
                logger.info(progress.get_completion_summary(
                    TimeUtils.get_current_datetime()
                ))

        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            raise

        finally:
            async with self._collection_lock:
                self._processing.remove(symbol)
                self._progress.pop(symbol, None)

    def get_collection_status(self) -> str:
        """Get collector status summary"""
        return (
            f"Active Collections: {len(self._processing)}\n"
            f"Queued Collections: {self._collection_queue.qsize()}\n"
            f"Last Collection Times:\n" +
            "\n".join(
                f"  {symbol}: {TimeUtils.from_timestamp(timestamp)}"
                for symbol, timestamp in self._last_collection.items()
            )
        )