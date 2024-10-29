# src/actions/symbol_actions.py

import asyncio
from typing import List, Dict, Any
from src.core.actions import Action, ActionContext, ActionValidationError
from src.core.events import EventType
from src.api.bybit_adapter import BybitAdapter
from src.managers.kline_manager import KlineManager
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

# src/actions/symbol_actions.py

class FetchSymbolsAction(Action):
    async def execute(self) -> None:
        session_factory = self.context.params["session_factory"]
        bybit = BybitAdapter()

        try:
            # Fetch currently active symbols
            active_symbols = set(await asyncio.to_thread(
                bybit.get_active_instruments
            ))

            if not active_symbols:
                logger.warning("No active symbols found")
                return

            # Process symbols
            with session_factory() as session:
                kline_manager = KlineManager(session)
                existing_symbols = set(kline_manager.get_all_symbols())

                # Find new and delisted symbols
                new_symbols = active_symbols - existing_symbols
                delisted_symbols = existing_symbols - active_symbols

                # Handle new symbols
                for symbol in new_symbols:
                    kline_manager.add_symbol(symbol)

                # Handle delisted symbols
                for symbol in delisted_symbols:
                    kline_manager.remove_symbol(symbol)

                # Commit changes
                session.commit()

                if new_symbols:
                    # Request historical data for new symbols
                    await self.emit_event(
                        EventType.HISTORICAL_DATA_NEEDED,
                        {
                            "symbols": list(new_symbols),
                            "reason": "new_symbols_found"
                        }
                    )

                if delisted_symbols:
                    logger.info(f"Removed {len(delisted_symbols)} delisted symbols")

                # Emit summary event
                await self.emit_event(
                    EventType.SYMBOLS_UPDATED,
                    {
                        "total_symbols": len(active_symbols),
                        "new_symbols": list(new_symbols),
                        "delisted_symbols": list(delisted_symbols),
                        "active_symbols": list(active_symbols)
                    }
                )

        except Exception as e:
            logger.error(f"Failed to fetch symbols: {e}")
            await self.emit_event(
                EventType.ACTION_FAILED,
                {
                    "action": "fetch_symbols",
                    "error": str(e)
                }
            )