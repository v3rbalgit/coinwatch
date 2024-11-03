# src/monitoring/actions/recovery.py
from typing import Any, Dict

from src.monitoring.actions.base import Action


class DatabaseRecoveryAction(Action):
    """Handle database recovery"""
    async def execute(self, context: Dict[str, Any]) -> bool:
        if context.get('connection_usage', 0) > 0.9:
            await self._scale_connection_pool(context)
        elif context.get('error_rate', 0) > 0.1:
            await self._reset_connections(context)
        return True

    async def validate(self, context: Dict[str, Any]) -> bool:
        return 'connection_usage' in context or 'error_rate' in context

class DataRepairAction(Action):
    """Handle data integrity issues"""
    async def execute(self, context: Dict[str, Any]) -> bool:
        if gaps := context.get('gaps'):
            await self._fill_data_gaps(gaps)
        if errors := context.get('validation_errors'):
            await self._fix_invalid_data(errors)
        return True

    async def validate(self, context: Dict[str, Any]) -> bool:
        return bool(context.get('gaps') or context.get('validation_errors'))