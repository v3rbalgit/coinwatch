# src/maintenance/observer.py

import asyncio
from typing import List
from ..services.base import ServiceBase

class SystemObserver:
    """System maintenance observer"""
    def __init__(self, services: List[ServiceBase]):
        self.services = services
        self.monitors = self._setup_monitors()

    def _setup_monitors(self) -> List['Monitor']:
        """Setup system monitors"""
        return [
            ResourceMonitor(),
            ConnectionMonitor(),
            StorageMonitor()
        ]

    async def start(self) -> None:
        """Start system monitoring"""
        for monitor in self.monitors:
            asyncio.create_task(monitor.observe())