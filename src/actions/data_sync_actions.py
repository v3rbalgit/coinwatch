# src/actions/data_sync_actions.py

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import asyncio
import time
from src.core.actions import Action, ActionValidationError
from src.core.events import Event, EventType
from src.managers.kline_manager import KlineManager
from src.managers.integrity_manager import DataIntegrityManager
from src.api.bybit_adapter import BybitAdapter
from src.utils.timestamp import get_current_timestamp, from_timestamp
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

@dataclass
class SyncResult:
    """Result of a synchronization operation."""
    symbol: str
    records_synced: int
    time_range: tuple[int, int]
    success: bool
    errors: Optional[List[str]] = None
    warnings: Optional[List[str]] = None

class BaseSyncAction(Action):
    """Base class for sync-related actions."""

    def __init__(self, context):
        super().__init__(context)
        self.bybit = BybitAdapter()

    async def _validate_symbol(self, symbol: str) -> None:
        """Validate symbol exists and is active."""
        try:
            active_symbols = await asyncio.to_thread(
                self.bybit.get_active_instruments
            )
            if symbol not in active_symbols:
                raise ActionValidationError(f"Symbol {symbol} is not active")
        except Exception as e:
            raise ActionValidationError(f"Failed to validate symbol: {str(e)}")

    async def _emit_sync_event(self,
                             event_type: EventType,
                             data: Dict[str, Any]) -> None:
        """Emit sync-related event with standard format."""
        await self.emit_event(event_type, {
            **data,
            'action_id': self.context.id,
            'timestamp': get_current_timestamp()
        })

class SyncSymbolDataAction(BaseSyncAction):
    """Synchronizes recent data for a symbol."""

    async def validate(self) -> None:
        """Validate sync parameters."""
        required = ['symbol', 'session_factory']
        for param in required:
            if param not in self.context.params:
                raise ActionValidationError(f"Missing required parameter: {param}")

        await self._validate_symbol(self.context.params['symbol'])

    async def execute(self) -> None:
        symbol = self.context.params['symbol']
        session_factory = self.context.params['session_factory']

        await self._emit_sync_event(
            EventType.DATA_SYNC_STARTED,
            {'symbol': symbol}
        )

        try:
            with session_factory() as session:
                kline_manager = KlineManager(session)

                # Get latest timestamp for symbol
                symbol_id = kline_manager.get_symbol_id(symbol)
                latest_ts = kline_manager.get_latest_timestamp(symbol_id)

                # Fetch and process new data
                current_time = get_current_timestamp()
                kline_data = await self._fetch_recent_data(
                    symbol,
                    latest_ts,
                    current_time
                )

                if kline_data:
                    # Insert new data
                    formatted_data = [
                        (int(item[0]), float(item[1]), float(item[2]),
                         float(item[3]), float(item[4]), float(item[5]),
                         float(item[6]))
                        for item in kline_data
                    ]

                    kline_manager.insert_kline_data(symbol_id, formatted_data)

                    # Emit completion event
                    await self._emit_sync_event(
                        EventType.DATA_SYNC_COMPLETED,
                        {
                            'symbol': symbol,
                            'records_synced': len(formatted_data),
                            'time_range': (latest_ts, current_time)
                        }
                    )
                else:
                    # Emit completion event with no changes
                    await self._emit_sync_event(
                        EventType.DATA_SYNC_COMPLETED,
                        {
                            'symbol': symbol,
                            'records_synced': 0,
                            'time_range': (latest_ts, current_time)
                        }
                    )

        except Exception as e:
            logger.error(f"Failed to sync data for {symbol}: {e}")
            await self._emit_sync_event(
                EventType.DATA_SYNC_FAILED,
                {
                    'symbol': symbol,
                    'error': str(e)
                }
            )
            raise

    async def _fetch_recent_data(self,
                                symbol: str,
                                start_time: int,
                                end_time: int,
                                batch_size: int = 200) -> List[List[Any]]:
        """Fetch recent kline data in batches."""
        try:
            response = await asyncio.to_thread(
                self.bybit.get_kline,
                symbol=symbol,
                interval='5',
                start_time=start_time,
                end_time=end_time,
                limit=batch_size
            )

            if response.get('retCode') == 0:
                return response.get('result', {}).get('list', [])
            return []

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return []

    async def rollback(self) -> None:
        """No rollback needed for sync operation."""
        pass

class FillDataGapAction(BaseSyncAction):
    """Fills identified gaps in historical data."""

    async def validate(self) -> None:
        required = ['symbol', 'gaps', 'session_factory']
        for param in required:
            if param not in self.context.params:
                raise ActionValidationError(f"Missing required parameter: {param}")

        await self._validate_symbol(self.context.params['symbol'])

        gaps = self.context.params['gaps']
        if not isinstance(gaps, list):
            raise ActionValidationError("Gaps must be a list")

        for gap in gaps:
            if 'start' not in gap or 'end' not in gap:
                raise ActionValidationError("Each gap must have start and end times")

    async def execute(self) -> None:
        symbol = self.context.params['symbol']
        gaps = self.context.params['gaps']
        session_factory = self.context.params['session_factory']

        try:
            with session_factory() as session:
                kline_manager = KlineManager(session)
                symbol_id = kline_manager.get_symbol_id(symbol)
                total_records = 0

                await self._emit_sync_event(
                    EventType.HISTORICAL_DATA_STARTED,
                    {
                        'symbol': symbol,
                        'gaps': gaps
                    }
                )

                for gap in gaps:
                    start_time = gap['start']
                    end_time = gap['end']

                    # Fetch and process gap data
                    gap_data = await self._fetch_gap_data(
                        symbol,
                        start_time,
                        end_time
                    )

                    if gap_data:
                        formatted_data = [
                            (int(item[0]), float(item[1]), float(item[2]),
                             float(item[3]), float(item[4]), float(item[5]),
                             float(item[6]))
                            for item in gap_data
                        ]

                        kline_manager.insert_kline_data(symbol_id, formatted_data)
                        total_records += len(formatted_data)

                        # Emit progress event
                        await self._emit_sync_event(
                            EventType.HISTORICAL_DATA_PROGRESS,
                            {
                                'symbol': symbol,
                                'gap_filled': {
                                    'start': start_time,
                                    'end': end_time,
                                    'records': len(formatted_data)
                                },
                                'total_records': total_records
                            }
                        )

                # Verify gap fill
                integrity_manager = DataIntegrityManager(session)
                verification = integrity_manager.verify_data_integrity(symbol)

                await self._emit_sync_event(
                    EventType.HISTORICAL_DATA_COMPLETED,
                    {
                        'symbol': symbol,
                        'total_records': total_records,
                        'remaining_gaps': verification.get('gaps', [])
                    }
                )

        except Exception as e:
            logger.error(f"Failed to fill gaps for {symbol}: {e}")
            await self._emit_sync_event(
                EventType.HISTORICAL_DATA_FAILED,
                {
                    'symbol': symbol,
                    'error': str(e)
                }
            )
            raise

    async def _fetch_gap_data(self,
                             symbol: str,
                             start_time: int,
                             end_time: int,
                             batch_size: int = 200) -> List[List[Any]]:
        """Fetch data for a specific gap."""
        all_data = []
        current_start = start_time

        while current_start < end_time:
            try:
                response = await asyncio.to_thread(
                    self.bybit.get_kline,
                    symbol=symbol,
                    interval='5',
                    start_time=current_start,
                    end_time=min(current_start + (batch_size * 5 * 60 * 1000), end_time),
                    limit=batch_size
                )

                if response.get('retCode') == 0:
                    data = response.get('result', {}).get('list', [])
                    all_data.extend(data)

                    if data:
                        current_start = int(data[-1][0]) + (5 * 60 * 1000)
                    else:
                        break
                else:
                    logger.warning(f"API returned error for {symbol}: {response}")
                    break

                await asyncio.sleep(0.1)  # Rate limiting

            except Exception as e:
                logger.error(f"Error fetching gap data for {symbol}: {e}")
                break

        return all_data

    async def rollback(self) -> None:
        """No rollback needed for gap fill operation."""
        pass

class DataSyncActionFactory:
    """Factory for creating data sync actions."""

    _actions = {
        'sync_symbol_data': SyncSymbolDataAction,
        'fill_data_gap': FillDataGapAction
    }

    @classmethod
    def create_action(cls, action_type: str, context) -> Action:
        """Create appropriate data sync action."""
        action_class = cls._actions.get(action_type)
        if not action_class:
            raise ValueError(f"Unknown action type: {action_type}")
        return action_class(context)