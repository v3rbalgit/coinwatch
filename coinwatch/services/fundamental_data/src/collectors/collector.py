from abc import ABC, abstractmethod
import asyncio
from typing import AsyncGenerator

from shared.core.models import MarketMetricsModel, MetadataModel, SentimentMetricsModel
from shared.utils.logger import LoggerSetup
from shared.utils.progress import FundamentalDataProgress
from shared.utils.time import get_current_timestamp, get_current_datetime, format_time_difference, to_timestamp



class FundamentalCollector[T: (MarketMetricsModel, MetadataModel, SentimentMetricsModel)](ABC):
    """
    Abstract base class for fundamental data collectors.

    Implements common functionality for collecting and managing fundamental data
    for different types of metrics (metadata, market, blockchain, sentiment).
    """

    def __init__(self, collection_interval: int):
        """
        Initialize the FundamentalCollector.

        Args:
            collection_interval (int): Time interval between collections in seconds.
        """
        self._collection_interval = collection_interval
        self._collection_lock = asyncio.Lock()
        self._running = False
        self._processing: set[str] = set()
        self._last_collection: dict[str, int] = {}  # Only used for status reporting
        self._current_progress: FundamentalDataProgress | None = None
        self.logger = LoggerSetup.setup(__class__.__name__)


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
    def fetch_token_data(self, tokens: set[str]) -> AsyncGenerator[T, None]:
        """
        Fetch fundamental data for specific tokens from an API.

        Args:
            tokens (Set[str]): List of tokens to collect data for.

        Yields:
            List[MarketMetricsModel | MetadataModel | SentimentMetricsModel]: Collected data for the tokens.
        """
        pass

    @abstractmethod
    async def store_token_data(self, data: list[T]) -> None:
        """
        Store collected data for tokens.

        Args:
            data (List[MarketMetricsModel | MetadataModel SentimentMetricsModel]): Collected data to store.
        """
        pass

    @abstractmethod
    async def get_token_data(self, tokens: set[str]) -> list[T]:
        """
        Get existing data for tokens stored in the database.

        Args:
            token (Set[str]): Tokens to get data for.

        Returns:
            List[MarketMetricsModel | MetadataModel SentimentMetricsModel]: Stored data for the tokens.
        """
        pass

    @abstractmethod
    async def delete_token_data(self, tokens: set[str]) -> None:
        """
        Delete data collection for tokens.

        Args:
            token (set[str]): token to delete.
        """
        pass


    async def add_tokens(self, tokens: set[str]) -> None:
        """
        Add a token to the collector.
        Used when a new token is added.

        Args:
            tokens (set[str]): Tokens to add
        """
        async with self._collection_lock:
            # Add to processing set so it will be included in next collection
            self._processing.update(tokens)


    async def remove_tokens(self, tokens: set[str]) -> None:
        """
        Remove tokens from collector and clean up their state.
        Used when tokens are delisted.

        Args:
            tokens (set[str]): Set of tokens to remove
        """
        async with self._collection_lock:
            # Remove from processing set
            self._processing.difference_update(tokens)

            # Clean up collection history
            for token in tokens:
                self._last_collection.pop(token, None)

            # Batch delete token data
            await self.delete_token_data(tokens)


    async def run(self) -> None:
        """Run continuous collection loop"""
        self._running = True

        while self._running:
            try:
                async with self._collection_lock:
                    await self._process_symbols(self._processing)

                # Wait until next collection interval
                await asyncio.sleep(self._collection_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Collection error: {e}")
                await asyncio.sleep(1)


    async def _process_symbols(self, tokens: set[str]) -> None:
        """
        Process a set of symbols by collecting and storing their data.

        Args:
            tokens (set[str]): Set of symbols to process.

        Raises:
            Exception: If an error occurs during processing. The error is logged and re-raised.
        """
        try:
            self.logger.info(f"Starting {self.collector_type} collection for {len(tokens)} tokens")

            tokens_to_collect = set()
            current_time = get_current_timestamp()

            # Get existing data for all tokens
            existing_data = await self.get_token_data(tokens)
            existing_data_map = {data.symbol: data for data in existing_data}

            # Check which tokens need collection based on data existence and update time
            for token in tokens:
                data = existing_data_map.get(token)

                if data:
                    # Check if data is within collection interval
                    time_since_update = current_time - to_timestamp(data.updated_at)

                    if time_since_update >= self._collection_interval * 1000:  # Convert to ms
                        tokens_to_collect.add(token)
                    else:
                        self.logger.info(f"{self.collector_type} up-to-date for '{data.symbol}', skipping...")
                else:
                    # No existing data, needs collection
                    tokens_to_collect.add(token)

            # Initialize progress for this collection batch
            self._current_progress = FundamentalDataProgress(
                collector_type=self.collector_type,
                start_time=get_current_datetime(),
                total_tokens=len(tokens_to_collect)
            )

            if tokens_to_collect:
                # Collect data in batches to handle failures gracefully
                collected_data = []
                failed_tokens = set()

                try:
                    async for data in self.fetch_token_data(tokens_to_collect):
                        try:
                            self._current_progress.processed_tokens += 1
                            self._current_progress.last_processed_token = data.symbol
                            self.logger.info(str(self._current_progress))
                            self._last_collection[data.symbol] = current_time
                            collected_data.append(data)
                        except Exception as e:
                            failed_tokens.add(data.symbol)
                            self.logger.error(f"Error processing token {data.symbol}: {e}")
                            continue

                    # Store all successfully collected data in one batch
                    if collected_data:
                        try:
                            await self.store_token_data(collected_data)
                        except Exception as e:
                            self.logger.error(f"Error storing collected data: {e}")
                            # Don't raise here to allow cleanup to proceed

                except Exception as e:
                    self.logger.error(f"Error during data collection: {e}")
                    # Don't raise here to allow cleanup to proceed

                if failed_tokens:
                    self.logger.warning(f"Failed to process tokens: {failed_tokens}")

            summary = self._current_progress.get_completion_summary()
            self.logger.info(summary)

        finally:
            self._current_progress = None


    async def cleanup(self) -> None:
        """Stop collection and cleanup"""
        self._running = False
        async with self._collection_lock:
            self._processing.clear()
            self._last_collection.clear()
        self._current_progress = None


    def get_collection_status(self) -> str:
        """
        Get a concise summary of the collector's current status.

        Returns:
            str: A formatted string with key collection status information.
        """
        current_time = get_current_timestamp()
        status_lines = [
            f"Collector: {self.collector_type}",
            f"Running: {self._running}",
            f"Active: {len(self._processing)}"
        ]

        if self._current_progress:
            status_lines.extend([
                "Current Collection Progress:",
                f"  Progress: {self._current_progress.processed_tokens}/{self._current_progress.total_tokens} tokens"
            ])

        status_lines.extend([
            "Last collections:",
            *[
                f"  {symbol}: {format_time_difference(current_time - timestamp)} ago"
                for symbol, timestamp in sorted(
                    self._last_collection.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:5]  # Show only the 5 most recent collections
            ]
        ])

        return "\n".join(status_lines)
