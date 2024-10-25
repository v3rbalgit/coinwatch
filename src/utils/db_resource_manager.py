# src/utils/db_resource_manager.py

from typing import Dict, Any, Optional
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from src.utils.db_retry import with_db_retry
from src.utils.logger import LoggerSetup
import threading
import time
import atexit

logger = LoggerSetup.setup(__name__)

class DatabaseResourceManager:
    """
    Manages and monitors database connection pool resources.

    Provides real-time monitoring of:
    - Connection pool utilization
    - Connection overflow detection
    - Pool health metrics

    Features:
    - Thread-safe monitoring
    - Automatic cleanup on exit
    - Configurable health checks
    """

    def __init__(self, engine: Engine):
        """
        Initialize resource manager with database engine.

        Args:
            engine: SQLAlchemy database engine with connection pool
        """
        self.engine = engine
        self._pool_stats: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self._monitor_thread: Optional[threading.Thread] = None
        self._running = False
        atexit.register(self.stop_monitoring)
        logger.debug("Database resource manager initialized")

    def start_monitoring(self) -> None:
        """
        Start the connection pool monitoring thread.

        Initializes a daemon thread that periodically checks:
        - Connection pool statistics
        - Pool health metrics
        - Resource utilization
        """
        if not self._running:
            self._running = True
            self._monitor_thread = threading.Thread(
                target=self._monitoring_loop,
                daemon=True,
                name="DBResourceMonitor"
            )
            self._monitor_thread.start()
            logger.info("Connection pool monitoring started")

    def stop_monitoring(self) -> None:
        """
        Stop the monitoring thread gracefully.

        Waits up to 5 seconds for the monitoring thread to complete.
        """
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
            logger.info("Connection pool monitoring stopped")

    def _monitoring_loop(self) -> None:
        """
        Main monitoring loop for database resources.

        Runs continuous monitoring with:
        - 30-second update interval
        - Automatic error recovery
        - Health check verification
        """
        while self._running:
            try:
                self.update_pool_stats()
                self._check_pool_health()
                time.sleep(30)  # Update interval
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}", exc_info=True)
                time.sleep(5)  # Brief delay before retry

    @with_db_retry(max_attempts=3)
    def update_pool_stats(self) -> None:
        """
        Update connection pool statistics.

        Collects:
        - Total connection count
        - Checked out connections
        - Overflow connections
        - Last update timestamp
        """
        with self._lock:
            if not isinstance(self.engine.pool, QueuePool):
                logger.warning("Non-QueuePool engine detected, metrics limited")
                return

            try:
                pool = self.engine.pool
                total_refs = len(getattr(pool, '_refs', []))

                pool_impl = getattr(pool, '_pool', None)
                max_pool_size = getattr(pool_impl, 'maxsize', 5) if pool_impl else 5

                self._pool_stats = {
                    'total_connections': total_refs,
                    'checked_out': total_refs,
                    'overflow': max(0, total_refs - max_pool_size),
                    'timestamp': time.time()
                }

                logger.debug(
                    f"Pool stats updated - Total: {total_refs}, "
                    f"Overflow: {max(0, total_refs - max_pool_size)}"
                )

            except Exception as e:
                logger.error(f"Failed to update pool stats: {e}", exc_info=True)
                self._pool_stats = {
                    'total_connections': 0,
                    'checked_out': 0,
                    'overflow': 0,
                    'timestamp': time.time(),
                    'error': str(e)
                }

    def _check_pool_health(self) -> None:
        """
        Check pool health metrics and log warnings.

        Monitors:
        - Connection overflow conditions
        - Excessive total connections
        - Resource utilization thresholds
        """
        with self._lock:
            stats = self._pool_stats

            overflow = stats.get('overflow', 0)
            if overflow > 0:
                logger.warning(
                    f"Pool overflow detected: {overflow} overflow connections"
                )

            total_connections = stats.get('total_connections', 0)
            if total_connections > 50:  # Connection threshold
                logger.warning(
                    f"High connection count: {total_connections} total connections"
                )

    def get_pool_stats(self) -> Dict[str, Any]:
        """
        Get current pool statistics.

        Returns:
            Dict containing:
                - total_connections: Total number of connections
                - checked_out: Currently checked out connections
                - overflow: Number of overflow connections
                - timestamp: Last update timestamp
                - error: Error message if update failed
        """
        with self._lock:
            return self._pool_stats.copy()