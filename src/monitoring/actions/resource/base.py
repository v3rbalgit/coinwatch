# src/monitoring/actions/resource/base.py

from typing import Dict, Any, Optional
from datetime import datetime
import asyncio

from ..base import Action
from .config import ResourceActionConfig
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)


class ResourceAction(Action):
    """Base class for resource-related actions"""

    def __init__(self, config: Optional[ResourceActionConfig]):
        self.config = config or ResourceActionConfig()
        self._last_execution: Dict[str, datetime] = {}

    async def execute(self, context: Dict[str, Any]) -> bool:
        """Execute the resource action with retries and timeout protection"""
        if not await self._check_cooldown():
            logger.info("Action in cooldown period, skipping execution")
            return False

        retries = 0
        while retries < self.config.max_retries:
            try:
                # Add timeout protection for the execution
                async with asyncio.timeout(300):  # 5 minute timeout
                    if await self._execute_action(context):
                        self._last_execution[self.__class__.__name__] = datetime.now()
                        return True

                retries += 1
                if retries < self.config.max_retries:
                    delay = self.config.retry_delay * (2 ** retries)  # Exponential backoff
                    logger.info(f"Action retry {retries}/{self.config.max_retries} after {delay}s")
                    await asyncio.sleep(delay)

            except asyncio.TimeoutError:
                logger.error(f"{self.__class__.__name__} action timed out")
                retries += 1
                if retries < self.config.max_retries:
                    await asyncio.sleep(self.config.retry_delay)

            except Exception as e:
                logger.error(f"Action execution failed: {e}", exc_info=True)
                retries += 1
                if retries < self.config.max_retries:
                    await asyncio.sleep(self.config.retry_delay)

        logger.error(f"Action failed after {self.config.max_retries} retries")
        return False

    async def _check_cooldown(self) -> bool:
        """Check if action is in cooldown period"""
        last_exec = self._last_execution.get(self.__class__.__name__)
        if last_exec:
            elapsed = (datetime.now() - last_exec).total_seconds()
            return elapsed >= self.config.cooldown_period
        return True

    async def validate(self, context: Dict[str, Any]) -> bool:
        """Validate resource context"""
        try:
            if not isinstance(context, dict):
                return False
            if not context.get('source') == 'resource':
                return False
            if not isinstance(context.get('metrics'), dict):
                return False
            return True
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False

    async def _execute_action(self, context: Dict[str, Any]) -> bool:
        """Template method for resource action execution.

        Args:
            context: Dictionary containing:
                - metrics: Current resource metrics
                - thresholds: Configured thresholds
                - source: Resource type (memory, cpu, disk)

        Returns:
            bool: True if action was successful, False otherwise

        Implementation requirements:
        - Must handle missing context values gracefully
        - Should log action outcomes
        - Should verify action effectiveness
        """
        raise NotImplementedError(
            f"Action {self.__class__.__name__} must implement _execute_action"
        )