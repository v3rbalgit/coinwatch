# src/utils/db_resource_manager.py

import logging
from typing import Dict, Any, Optional
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from src.utils.db_retry import with_db_retry
import threading
import time
import atexit

logger = logging.getLogger(__name__)

class DatabaseResourceManager:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._pool_stats: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self._monitor_thread: Optional[threading.Thread] = None
        self._running = False
        atexit.register(self.stop_monitoring)

    def start_monitoring(self) -> None:
        """Start monitoring database connection pool."""
        if not self._running:
            self._running = True
            self._monitor_thread = threading.Thread(
                target=self._monitoring_loop,
                daemon=True,
                name="DBResourceMonitor"
            )
            self._monitor_thread.start()
            logger.info("Database resource monitoring started")

    def stop_monitoring(self) -> None:
        """Stop monitoring database connection pool."""
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
            logger.info("Database resource monitoring stopped")

    def _monitoring_loop(self) -> None:
        """Main monitoring loop for database resources."""
        while self._running:
            try:
                self.update_pool_stats()
                self._check_pool_health()
                time.sleep(30)  # Update every 30 seconds
            except Exception as e:
                logger.error(f"Error in database resource monitoring: {e}")
                time.sleep(5)

    @with_db_retry(max_attempts=3)
    def update_pool_stats(self) -> None:
        """Update database connection pool statistics."""
        with self._lock:
            if not isinstance(self.engine.pool, QueuePool):
                logger.warning("Engine pool is not a QueuePool, some metrics unavailable")
                return

            try:
                pool = self.engine.pool
                total_refs = len(getattr(pool, '_refs', []))

                # Get pool size with a default value
                pool_impl = getattr(pool, '_pool', None)
                max_pool_size = getattr(pool_impl, 'maxsize', 5) if pool_impl else 5

                self._pool_stats = {
                    'total_connections': total_refs,
                    'checked_out': total_refs,  # Current checked out connections
                    'overflow': max(0, total_refs - max_pool_size),
                    'timestamp': time.time()
                }
            except Exception as e:
                logger.error(f"Error updating pool stats: {e}")
                self._pool_stats = {
                    'total_connections': 0,
                    'checked_out': 0,
                    'overflow': 0,
                    'timestamp': time.time(),
                    'error': str(e)
                }

    def _check_pool_health(self) -> None:
        """Check pool health and log warnings."""
        with self._lock:
            stats = self._pool_stats

            # Check for high connection usage
            if stats.get('overflow', 0) > 0:
                logger.warning(f"Connection pool overflow detected: {stats['overflow']} overflow connections")

            # Check for too many connections
            if stats.get('total_connections', 0) > 50:  # Adjust threshold as needed
                logger.warning(f"High number of total connections: {stats['total_connections']}")

    def get_pool_stats(self) -> Dict[str, Any]:
        """Get current pool statistics."""
        with self._lock:
            return self._pool_stats.copy()