# src/services/monitor_service.py

import logging
from typing import Dict, List, TypedDict, Any
from datetime import datetime
from sqlalchemy import text, Engine
from sqlalchemy.orm import Session
import json
from pathlib import Path
from utils.db_resource_manager import DatabaseResourceManager
from utils.db_retry import with_db_retry
from services.data_quality_service import DataQualityMetrics
import atexit

logger = logging.getLogger(__name__)

class TableStats(TypedDict):
    name: str
    rows: int
    size_mb: float
    index_size_mb: float
    total_size_mb: float

class DatabaseStats(TypedDict):
    timestamp: int
    total_size_mb: float
    tables: List[TableStats]
    pool_stats: Dict[str, Any]
    data_quality: Dict[str, Any]

class StorageThresholds(TypedDict):
    warning_gb: float
    critical_gb: float

class MonitoringConfig(TypedDict):
    thresholds: StorageThresholds
    check_interval_hours: int
    retention_days: int

class DatabaseMonitor:
    DEFAULT_CONFIG: MonitoringConfig = {
        'thresholds': {
            'warning_gb': 50.0,
            'critical_gb': 70.0
        },
        'check_interval_hours': 24,
        'retention_days': 30
    }

    def __init__(self, session: Session, stats_dir: str = "stats"):
        self.session = session
        self.stats_dir = Path(stats_dir)
        self.stats_dir.mkdir(exist_ok=True)
        self.stats_file = self.stats_dir / "db_stats.json"
        self.config_file = self.stats_dir / "monitor_config.json"
        self._load_config()

        # Initialize resource managers
        if hasattr(session, 'bind'):
            engine = session.bind
            if isinstance(engine, Engine):  # Add type check
                self.db_resource_manager = DatabaseResourceManager(engine)
            else:
                logger.warning("Session bind is not an Engine instance")
                self.db_resource_manager = None
        else:
            logger.warning("Session has no engine binding, resource monitoring limited")
            self.db_resource_manager = None

        atexit.register(self.cleanup)

    def _load_config(self) -> None:
        """Load monitoring configuration from file or create default."""
        try:
            if self.config_file.exists():
                with open(self.config_file, 'r') as f:
                    self.config: MonitoringConfig = json.load(f)
            else:
                self.config = self.DEFAULT_CONFIG
                with open(self.config_file, 'w') as f:
                    json.dump(self.config, f, indent=2)
        except Exception as e:
            logger.error(f"Error loading config, using defaults: {e}")
            self.config = self.DEFAULT_CONFIG

    def cleanup(self) -> None:
        """Cleanup resources on shutdown."""
        try:
            if self.db_resource_manager:
                self.db_resource_manager.stop_monitoring()
        except Exception as e:
            logger.error(f"Error during monitor cleanup: {e}")

    def check_alerts(self, current_size_mb: float) -> List[str]:
        """Check if any storage thresholds have been exceeded."""
        alerts = []
        current_size_gb = current_size_mb / 1024
        thresholds = self.config['thresholds']

        if current_size_gb >= thresholds['critical_gb']:
            alerts.append(f"CRITICAL: Database size ({current_size_gb:.1f}GB) exceeds critical threshold "
                        f"of {thresholds['critical_gb']}GB")
        elif current_size_gb >= thresholds['warning_gb']:
            alerts.append(f"WARNING: Database size ({current_size_gb:.1f}GB) exceeds warning threshold "
                        f"of {thresholds['warning_gb']}GB")

        return alerts

    @with_db_retry(max_attempts=3)
    def get_table_stats(self) -> List[TableStats]:
        """Get statistics for all tables in the database."""
        try:
            query = text("""
                SELECT
                    table_name as name,
                    table_rows as rows,
                    data_length/(1024*1024) as size_mb,
                    index_length/(1024*1024) as index_size_mb,
                    (data_length + index_length)/(1024*1024) as total_size_mb
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                  AND table_type = 'BASE TABLE'
            """)

            result = self.session.execute(query)
            return [TableStats(
                name=row.name,
                rows=row.rows or 0,
                size_mb=float(row.size_mb or 0),
                index_size_mb=float(row.index_size_mb or 0),
                total_size_mb=float(row.total_size_mb or 0)
            ) for row in result]

        except Exception as e:
            logger.error(f"Error getting table statistics: {e}")
            return []

    def save_stats(self, stats: DatabaseStats) -> None:
        """Save database statistics to file."""
        try:
            if self.stats_file.exists():
                with open(self.stats_file, 'r') as f:
                    data = json.load(f)
            else:
                data = {'history': []}

            # Keep last 30 days of statistics
            retention_period = self.config['retention_days'] * 24 * 60 * 60 * 1000
            data['history'] = ([stat for stat in data['history']
                              if stat['timestamp'] > stats['timestamp'] - retention_period]
                             + [stats])

            with open(self.stats_file, 'w') as f:
                json.dump(data, f, indent=2)

        except Exception as e:
            logger.error(f"Error saving statistics: {e}")

    @with_db_retry(max_attempts=3)
    def collect_stats(self) -> None:
        """Collect and save current database statistics."""
        try:
            tables = self.get_table_stats()
            total_size = sum(table['total_size_mb'] for table in tables)

            # Get pool statistics if available
            pool_stats = {}
            if self.db_resource_manager:
                pool_stats = self.db_resource_manager.get_pool_stats()

            # Get data quality metrics
            quality_service = DataQualityMetrics(self.session)
            system_quality = quality_service.get_system_quality_metrics()

            stats: DatabaseStats = {
                'timestamp': int(datetime.now().timestamp() * 1000),
                'total_size_mb': total_size,
                'tables': tables,
                'pool_stats': pool_stats,
                'data_quality': system_quality
            }

            self.save_stats(stats)

            # Check for alerts
            alerts = self.check_alerts(total_size)

            # Add data quality alerts
            if system_quality.get('system_stats', {}).get('symbol_count', 0) == 0:
                alerts.append("WARNING: No symbols found in database")

            # Add integrity alerts
            overall_integrity = system_quality.get('overall_integrity', {})
            if overall_integrity.get('symbols_with_gaps', 0) > 0:
                alerts.append(f"WARNING: {overall_integrity['symbols_with_gaps']} symbols have data gaps")
            if overall_integrity.get('symbols_with_duplicates', 0) > 0:
                alerts.append(f"WARNING: {overall_integrity['symbols_with_duplicates']} symbols have duplicate records")

            for alert in alerts:
                logger.warning(alert)

            # Log metrics
            logger.info(f"Current database size: {total_size:.2f} MB")
            logger.info(f"Symbols tracked: {system_quality.get('system_stats', {}).get('symbol_count', 0)}")
            logger.info(f"Total records: {system_quality.get('system_stats', {}).get('total_records', 0)}")
            logger.info("Data integrity status:")
            logger.info(f"- Symbols with gaps: {overall_integrity.get('symbols_with_gaps', 0)}")
            logger.info(f"- Symbols with duplicates: {overall_integrity.get('symbols_with_duplicates', 0)}")

            # Log connection pool stats
            if pool_stats:
                logger.info(
                    f"Connection pool status - "
                    f"Total: {pool_stats.get('total_connections', 0)}, "
                    f"Checked out: {pool_stats.get('checked_out', 0)}, "
                    f"Overflow: {pool_stats.get('overflow', 0)}"
                )

        except Exception as e:
            logger.error(f"Error collecting database statistics: {e}")