# src/services/data_quality_service.py

import logging
from typing import Dict
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.orm import Session
from utils.timestamp import from_timestamp

logger = logging.getLogger(__name__)

class DataQualityMetrics:
    def __init__(self, session: Session):
        self.session = session

    def check_data_integrity(self, symbol_id: int, time_window: int = 24) -> Dict:
        """
        Check data integrity for a symbol.

        Args:
            symbol_id: The symbol ID to check
            time_window: Hours to look back

        Returns:
            Dict containing integrity metrics
        """
        try:
            end_time = int(datetime.now().timestamp() * 1000)
            start_time = end_time - (time_window * 60 * 60 * 1000)

            query = text("""
                SELECT
                    COUNT(*) as record_count,
                    COUNT(DISTINCT start_time) as unique_timestamps,
                    MAX(start_time) - MIN(start_time) as time_span,
                    COUNT(*) * 300000 as expected_timespan  -- 5 minutes in milliseconds
                FROM kline_data
                WHERE symbol_id = :symbol_id
                AND start_time BETWEEN :start_time AND :end_time
            """)

            result = self.session.execute(query, {
                'symbol_id': symbol_id,
                'start_time': start_time,
                'end_time': end_time
            }).first()

            if result is None:
                return {}

            return {
                'expected_records': (end_time - start_time) // (5 * 60 * 1000),  # Expected 5min intervals
                'actual_records': result.record_count if result else 0,
                'unique_timestamps': result.unique_timestamps if result else 0,
                'has_duplicates': result.record_count != result.unique_timestamps if result else False,
                'time_coverage': f"{(result.time_span / result.expected_timespan * 100):.2f}%" if result and result.expected_timespan else "0%"
            }

        except Exception as e:
            logger.error(f"Error checking data integrity: {e}")
            return {}

    def get_system_quality_metrics(self) -> Dict:
        """Get system-wide data quality metrics including integrity checks."""
        try:
            # Get basic system stats
            base_query = text("""
                SELECT
                    COUNT(DISTINCT s.id) as symbol_count,
                    COUNT(*) as total_records,
                    MIN(k.start_time) as oldest_record,
                    MAX(k.start_time) as newest_record
                FROM symbols s
                JOIN kline_data k ON k.symbol_id = s.id
            """)

            system_stats = self.session.execute(base_query).first()
            if system_stats is None:
                logger.warning("No system statistics available")
                return {}

            # Get integrity metrics for last 24 hours
            integrity_metrics = {}
            if system_stats.symbol_count > 0:
                symbols_query = text("SELECT id FROM symbols")
                symbol_ids = [row[0] for row in self.session.execute(symbols_query)]

                for symbol_id in symbol_ids:
                    integrity = self.check_data_integrity(symbol_id)
                    if integrity:
                        integrity_metrics[str(symbol_id)] = integrity

            return {
                'system_stats': {
                    'symbol_count': system_stats.symbol_count if system_stats else 0,
                    'total_records': system_stats.total_records if system_stats else 0,
                    'data_range': {
                        'start': from_timestamp(system_stats.oldest_record) if system_stats and system_stats.oldest_record else None,
                        'end': from_timestamp(system_stats.newest_record) if system_stats and system_stats.newest_record else None
                    }
                },
                'integrity_metrics': integrity_metrics,
                'overall_integrity': {
                    'symbols_with_gaps': sum(1 for m in integrity_metrics.values()
                                          if m.get('actual_records', 0) < m.get('expected_records', 0)),
                    'symbols_with_duplicates': sum(1 for m in integrity_metrics.values()
                                                if m.get('has_duplicates', False))
                }
            }

        except Exception as e:
            logger.error(f"Error getting system quality metrics: {e}")
            return {}