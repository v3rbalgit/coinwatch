# src/actions/data_actions.py

from typing import List, Any, Optional
import asyncio
from datetime import datetime, timezone
from src.core.actions import Action, ActionContext, ActionValidationError
from src.core.events import EventType
from src.api.bybit_adapter import BybitAdapter
from src.utils.timestamp import get_current_timestamp
from src.managers.kline_manager import KlineManager
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class FetchHistoricalDataAction(Action):
    """Fetches historical data for specified symbols."""

    async def validate(self) -> None:
        """Validate action parameters."""
        required_params = ["symbols", "session_factory"]
        for param in required_params:
            if param not in self.context.params:
                raise ActionValidationError(f"Missing required parameter: {param}")

        symbols = self.context.params["symbols"]
        if not isinstance(symbols, (list, tuple)):
            raise ActionValidationError("Symbols must be a list")

        # Validate time range if provided
        start_time = self.context.params.get("start_time")
        end_time = self.context.params.get("end_time")
        if start_time and end_time and start_time >= end_time:
            raise ActionValidationError("Start time must be before end time")

    async def execute(self) -> None:
        """Execute historical data fetch."""
        symbols = self.context.params["symbols"]
        session_factory = self.context.params["session_factory"]
        start_time = self.context.params.get("start_time")
        end_time = self.context.params.get("end_time", get_current_timestamp())
        batch_size = self.context.params.get("batch_size", 200)

        bybit = BybitAdapter()
        total_symbols = len(symbols)
        processed = 0

        for symbol in symbols:
            try:
                await self.emit_event(
                    EventType.HISTORICAL_DATA_STARTED,
                    {
                        "symbol": symbol,
                        "start_time": start_time,
                        "end_time": end_time
                    }
                )

                with session_factory() as session:
                    kline_manager = KlineManager(session)
                    symbol_id = kline_manager.get_symbol_id(symbol)

                    if start_time is None:
                        start_time = await self._get_symbol_start_time(bybit, symbol)

                    current_start = start_time
                    while current_start < end_time:
                        chunk_end = min(
                            current_start + (batch_size * 5 * 60 * 1000),
                            end_time
                        )

                        try:
                            kline_data = await self._fetch_kline_chunk(
                                bybit, symbol, current_start, chunk_end, batch_size
                            )

                            if kline_data:
                                formatted_data = [
                                    (int(item[0]), float(item[1]), float(item[2]),
                                     float(item[3]), float(item[4]), float(item[5]),
                                     float(item[6]))
                                    for item in kline_data
                                ]

                                kline_manager.insert_kline_data(symbol_id, formatted_data)

                                # Update progress
                                progress = ((current_start - start_time) /
                                          (end_time - start_time) * 100)
                                await self._emit_progress(
                                    symbol, progress, processed, total_symbols
                                )

                        except Exception as e:
                            logger.error(f"Error processing chunk for {symbol}: {e}")

                        current_start = chunk_end + 1
                        await asyncio.sleep(0.1)

                processed += 1
                await self.emit_event(
                    EventType.HISTORICAL_DATA_COMPLETED,
                    {
                        "symbol": symbol,
                        "success": True
                    }
                )

            except Exception as e:
                logger.error(f"Failed to process symbol {symbol}: {e}")
                await self.emit_event(
                    EventType.HISTORICAL_DATA_COMPLETED,
                    {
                        "symbol": symbol,
                        "success": False,
                        "error": str(e)
                    }
                )

    async def rollback(self) -> None:
        """No rollback needed as data fetch is idempotent."""
        pass

    async def _get_symbol_start_time(self,
                               bybit: BybitAdapter,
                               symbol: str) -> int:
        """Get the symbol's first trade time."""
        try:
            # The API call returns a tuple of (response_data, rate_limit_info, response_headers)
            response_tuple = await asyncio.to_thread(
                bybit.session.get_instruments_info,
                category='linear',
                symbol=symbol
            )

            # Extract the actual response data from the tuple
            response_data = response_tuple[0] if isinstance(response_tuple, tuple) else response_tuple

            if isinstance(response_data, dict) and response_data.get('retCode') == 0:
                data = response_data.get('result', {}).get('list', [])
                if data and 'launchTime' in data[0]:
                    return int(data[0]['launchTime'])

            # Default to a recent time if can't get launch time
            return int((datetime.now(timezone.utc)
                        .timestamp() * 1000)) - (7 * 24 * 60 * 60 * 1000)
        except Exception as e:
            logger.error(f"Error getting start time for {symbol}: {e}")
            raise

    async def _fetch_kline_chunk(self,
                                bybit: BybitAdapter,
                                symbol: str,
                                start_time: int,
                                end_time: int,
                                limit: int) -> Optional[List[List[Any]]]:
        """Fetch a chunk of kline data."""
        try:
            response = await asyncio.to_thread(
                bybit.get_kline,
                symbol=symbol,
                interval='5',
                start_time=start_time,
                end_time=end_time,
                limit=limit
            )

            if response.get('retCode') == 0:
                return response.get('result', {}).get('list', [])
            return None

        except Exception as e:
            logger.error(
                f"Error fetching kline chunk for {symbol}: {e}"
            )
            return None

    async def _emit_progress(self,
                           symbol: str,
                           symbol_progress: float,
                           processed: int,
                           total: int) -> None:
        """Emit progress event."""
        await self.emit_event(
            EventType.HISTORICAL_DATA_PROGRESS,
            {
                "symbol": symbol,
                "symbol_progress": symbol_progress,
                "total_progress": (processed / total) * 100,
                "processed_symbols": processed,
                "total_symbols": total
            }
        )

class SyncRecentDataAction(Action):
    """Synchronizes recent data for specified symbols."""

    async def validate(self) -> None:
        """Validate action parameters."""
        required_params = ["symbols", "session_factory"]
        for param in required_params:
            if param not in self.context.params:
                raise ActionValidationError(f"Missing required parameter: {param}")

        symbols = self.context.params["symbols"]
        if not isinstance(symbols, (list, tuple)):
            raise ActionValidationError("Symbols must be a list")

    async def execute(self) -> None:
        """Execute recent data synchronization."""
        symbols = self.context.params["symbols"]
        session_factory = self.context.params["session_factory"]
        end_time = get_current_timestamp()
        bybit = BybitAdapter()

        for symbol in symbols:
            try:
                with session_factory() as session:
                    kline_manager = KlineManager(session)
                    symbol_id = kline_manager.get_symbol_id(symbol)

                    # Get latest timestamp for symbol
                    latest_ts = kline_manager.get_latest_timestamp(symbol_id)
                    if latest_ts is None:
                        logger.warning(
                            f"No existing data for {symbol}, "
                            "initiating historical fetch"
                        )
                        await self._request_historical_fetch(symbol)
                        continue

                    # Fetch recent data
                    kline_data = await self._fetch_kline_chunk(
                        bybit, symbol, latest_ts, end_time
                    )

                    if kline_data:
                        formatted_data = [
                            (int(item[0]), float(item[1]), float(item[2]),
                             float(item[3]), float(item[4]), float(item[5]),
                             float(item[6]))
                            for item in kline_data
                        ]

                        kline_manager.insert_kline_data(symbol_id, formatted_data)

                        await self.emit_event(
                            EventType.DATA_SYNC_COMPLETED,
                            {
                                "symbol": symbol,
                                "records_updated": len(formatted_data)
                            }
                        )

            except Exception as e:
                logger.error(f"Failed to sync recent data for {symbol}: {e}")
                await self.emit_event(
                    EventType.DATA_SYNC_FAILED,
                    {
                        "symbol": symbol,
                        "error": str(e)
                    }
                )

    async def rollback(self) -> None:
        """No rollback needed for sync operation."""
        pass

    async def _fetch_kline_chunk(self,
                                bybit: BybitAdapter,
                                symbol: str,
                                start_time: int,
                                end_time: int,
                                limit: int = 200) -> Optional[List[List[Any]]]:
        """Fetch a chunk of kline data."""
        try:
            response = await asyncio.to_thread(
                bybit.get_kline,
                symbol=symbol,
                interval='5',
                start_time=start_time,
                end_time=end_time,
                limit=limit
            )

            if response.get('retCode') == 0:
                return response.get('result', {}).get('list', [])
            return None

        except Exception as e:
            logger.error(
                f"Error fetching kline chunk for {symbol}: {e}"
            )
            return None

    async def _request_historical_fetch(self, symbol: str) -> None:
        """Request historical data fetch for symbol."""
        await self.emit_event(
            EventType.HISTORICAL_DATA_NEEDED,
            {
                "symbols": [symbol],
                "reason": "no_existing_data"
            }
        )