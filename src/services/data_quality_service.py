# src/services/data_quality_service.py

from typing import Dict, Optional, TypedDict, Any
from datetime import datetime, timezone
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.engine import Row
from src.utils.timestamp import from_timestamp
from src.utils.logger import LoggerSetup
from src.utils.db_retry import with_db_retry

logger = LoggerSetup.setup(__name__)

class SystemStats(TypedDict):
    symbol_count: int
    total_records: int
    oldest_record: Optional[int]
    newest_record: Optional[int]

class DataQualityMetrics:
    """
    Monitors and reports on data quality metrics across the system.

    Provides comprehensive data quality analysis including:
    - Data integrity checks for individual symbols
    - Gap detection in time series data
    - Duplicate record detection
    - Orphaned record tracking
    - Time coverage analysis
    - System-wide quality metrics

    The class uses SQLAlchemy for database operations and includes retry mechanisms
    for resilient database interactions.
    """

    def __init__(self, session: Session):
        """
        Initialize DataQualityMetrics with database session.

        Args:
            session: SQLAlchemy session for database operations
        """
        self.session = session
        self.MS_PER_5MIN = 5 * 60 * 1000  # 5 minutes in milliseconds
        logger.debug("Data quality metrics initialized")

    @with_db_retry(max_attempts=3)
    def check_data_integrity(self, symbol_id: int, time_window: int = 24) -> Dict:
        """
        Check data integrity for a specific symbol over a given time window.

        Performs comprehensive integrity checks including:
        - Record count verification
        - Duplicate detection
        - Time coverage analysis
        - Gap detection

        Args:
            symbol_id: Database ID of the symbol to check
            time_window: Number of hours to look back (default: 24)

        Returns:
            Dict containing:
                - expected_records: Expected number of 5-min intervals
                - actual_records: Actual number of records found
                - unique_timestamps: Number of unique timestamps
                - has_duplicates: Whether duplicates were found
                - time_coverage: Percentage of time covered
                - gaps_found: Whether any time gaps were detected
                - coverage_ratio: Ratio of actual to expected coverage
        """
        try:
            end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
            start_time = end_time - (time_window * 60 * 60 * 1000)

            logger.debug(
                f"Checking integrity for symbol {symbol_id} from "
                f"{from_timestamp(start_time)} to {from_timestamp(end_time)}"
            )

            query = text("""
                SELECT
                    COUNT(*) as record_count,
                    COUNT(DISTINCT start_time) as unique_timestamps,
                    MAX(start_time) - MIN(start_time) as time_span,
                    COUNT(*) * :interval_ms as expected_timespan
                FROM kline_data
                WHERE symbol_id = :symbol_id
                AND start_time BETWEEN :start_time AND :end_time
            """)

            result = self.session.execute(query, {
                'symbol_id': symbol_id,
                'start_time': start_time,
                'end_time': end_time,
                'interval_ms': self.MS_PER_5MIN
            }).first()

            if result is None:
                logger.warning(f"No data found for symbol {symbol_id}")
                return {}

            expected_records = (end_time - start_time) // self.MS_PER_5MIN
            actual_records = result.record_count
            unique_timestamps = result.unique_timestamps
            time_span = result.time_span or 0
            expected_timespan = result.expected_timespan or 1  # Avoid division by zero

            metrics = {
                'expected_records': expected_records,
                'actual_records': actual_records,
                'unique_timestamps': unique_timestamps,
                'has_duplicates': actual_records != unique_timestamps,
                'time_coverage': f"{(time_span / expected_timespan * 100):.2f}%",
                'gaps_found': actual_records < expected_records,
                'coverage_ratio': time_span / expected_timespan
            }

            logger.debug(f"Integrity check results for symbol {symbol_id}: {metrics}")
            return metrics

        except Exception as e:
            logger.error(f"Failed to check data integrity for symbol {symbol_id}: {e}")
            return {}

    @with_db_retry(max_attempts=3)
    def get_system_quality_metrics(self) -> Dict[str, Any]:
        """
        Get comprehensive system-wide data quality metrics.

        Collects and analyzes:
        - System-wide statistics
        - Per-symbol integrity metrics
        - Orphaned record counts
        - Overall data quality indicators

        Returns:
            Dict containing:
                - system_stats: Basic system statistics (symbol count, record count, date range)
                - integrity_metrics: Per-symbol integrity check results
                - overall_integrity: System-wide integrity summary
                - data_quality: Quality indicators and timestamp of check
        """
        try:
            # Check for orphaned records
            orphaned_count = self._get_orphaned_records_count()
            logger.debug(f"Found {orphaned_count:,} orphaned records")

            # Get basic system statistics
            system_stats = self._get_system_stats()
            if not system_stats:
                logger.warning("No system statistics available")
                return {}

            # Build comprehensive quality report
            quality_report = {
                'system_stats': {
                    'symbol_count': system_stats.symbol_count,
                    'total_records': system_stats.total_records,
                    'data_range': self._format_data_range(system_stats)
                },
                'integrity_metrics': self._collect_integrity_metrics(),
                'overall_integrity': self._calculate_overall_integrity(
                    integrity_metrics := self._collect_integrity_metrics(),
                    orphaned_count
                ),
                'data_quality': {
                    'orphaned_records_percentage': self._calculate_orphaned_percentage(
                        orphaned_count, system_stats.total_records
                    ),
                    'last_check_time': datetime.now(timezone.utc).isoformat()
                }
            }

            logger.info(self._format_quality_summary(quality_report))
            return quality_report

        except Exception as e:
            logger.error("Failed to get system quality metrics", exc_info=e)
            return {}

    def _get_orphaned_records_count(self) -> int:
        """
        Get count of orphaned records in the database.

        Orphaned records are kline entries that reference non-existent symbols.

        Returns:
            int: Number of orphaned records found
        """
        result = self.session.execute(text("""
            SELECT COUNT(*) as orphaned
            FROM kline_data k
            WHERE NOT EXISTS (
                SELECT 1 FROM symbols s
                WHERE s.id = k.symbol_id
            )
        """))
        return result.scalar() or 0

    def _get_system_stats(self) -> Optional[Row[Any]]:
        """
        Get basic system statistics from the database.

        Retrieves:
        - Total symbol count
        - Total record count
        - Oldest and newest record timestamps

        Returns:
            Optional[Row[Any]]: SQLAlchemy result row containing:
                - symbol_count: Number of symbols in database
                - total_records: Total number of kline records
                - oldest_record: Timestamp of oldest record
                - newest_record: Timestamp of newest record
                Returns None if no data is found
        """
        result = self.session.execute(text("""
            SELECT
                COUNT(DISTINCT s.id) as symbol_count,
                COUNT(k.id) as total_records,
                MIN(k.start_time) as oldest_record,
                MAX(k.start_time) as newest_record
            FROM symbols s
            LEFT JOIN kline_data k ON k.symbol_id = s.id
        """)).first()
        return result

    def _collect_integrity_metrics(self) -> Dict:
        """
        Collect integrity metrics for all symbols in the database.

        Performs integrity checks on each symbol independently and
        aggregates the results.

        Returns:
            Dict: Mapping of symbol IDs to their integrity metrics
        """
        integrity_metrics = {}
        symbol_ids = [row[0] for row in self.session.execute(text("SELECT id FROM symbols"))]

        for symbol_id in symbol_ids:
            metrics = self.check_data_integrity(symbol_id)
            if metrics:
                integrity_metrics[str(symbol_id)] = metrics

        return integrity_metrics

    def _format_data_range(self, stats: Row[Any]) -> Dict[str, Optional[datetime]]:
        """
        Format data range timestamps into human-readable format.

        Takes raw timestamp data and converts it to datetime objects
        for better readability and analysis.

        Args:
            stats: SQLAlchemy Row containing oldest_record and newest_record timestamps

        Returns:
            Dict[str, Optional[datetime]]: Formatted date range containing:
                - start: Start datetime or None if no data
                - end: End datetime or None if no data
        """
        return {
            'start': from_timestamp(stats.oldest_record) if stats.oldest_record else None,
            'end': from_timestamp(stats.newest_record) if stats.newest_record else None
        }

    def _calculate_overall_integrity(self, integrity_metrics: Dict, orphaned_count: int) -> Dict:
        """
        Calculate system-wide integrity metrics.

        Aggregates individual symbol integrity metrics and combines with
        orphaned record information to provide system-wide overview.

        Args:
            integrity_metrics: Dictionary of per-symbol integrity metrics
            orphaned_count: Number of orphaned records found

        Returns:
            Dict containing:
                - symbols_with_gaps: Number of symbols having time gaps
                - symbols_with_duplicates: Number of symbols with duplicate records
                - orphaned_records: Total count of orphaned records
        """

        return {
            'symbols_with_gaps': sum(1 for m in integrity_metrics.values()
                                   if m.get('gaps_found', False)),
            'symbols_with_duplicates': sum(1 for m in integrity_metrics.values()
                                         if m.get('has_duplicates', False)),
            'orphaned_records': orphaned_count
        }

    def _calculate_orphaned_percentage(self, orphaned_count: int, total_records: int) -> float:
        """
        Calculate percentage of records that are orphaned.

        Args:
            orphaned_count: Number of orphaned records
            total_records: Total number of records in system

        Returns:
            float: Percentage of orphaned records (0-100)
                  Returns 0 if total_records is 0
        """
        return (orphaned_count / total_records * 100) if total_records > 0 else 0

    def _format_quality_summary(self, report: Dict) -> str:
        """
        Format quality report data into human-readable summary string.

        Creates a multi-line summary string highlighting key metrics
        from the quality report.

        Args:
            report: Quality report dictionary containing system stats
                   and integrity metrics

        Returns:
            str: Formatted summary string containing:
                - Total symbol count
                - Total record count
                - Number of symbols with gaps
                - Number of symbols with duplicates
                - Number of orphaned records
        """
        return (
            f"Data Quality Summary:\n"
            f"- Symbols: {report['system_stats']['symbol_count']:,}\n"
            f"- Total Records: {report['system_stats']['total_records']:,}\n"
            f"- Symbols with Gaps: {report['overall_integrity']['symbols_with_gaps']}\n"
            f"- Symbols with Duplicates: {report['overall_integrity']['symbols_with_duplicates']}\n"
            f"- Orphaned Records: {report['overall_integrity']['orphaned_records']:,}"
        )