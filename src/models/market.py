# src/models/market.py

import os
from typing import List
import sqlalchemy as sa
from sqlalchemy import Index, DDL, BigInteger, Float, String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.event import listen
from .base import Base

class Symbol(Base):
    """
    Symbol model for database

    Represents trading pairs available on exchanges.
    Contains metadata about each trading instrument.
    """
    __tablename__ = 'symbols'

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    exchange: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    first_trade_time: Mapped[int] = mapped_column(BigInteger, nullable=True)

    # Add composite index for faster lookups
    __table_args__ = (
        UniqueConstraint('name', 'exchange', name='uix_symbol_exchange'),
        Index('idx_symbol_lookup', 'name', 'exchange')
    )

    # Define the relationship to Kline
    klines: Mapped[List['Kline']] = relationship(
        'Kline',
        back_populates='symbol',
        cascade='all, delete-orphan'
    )

    def __repr__(self) -> str:
        return f"Symbol(id={self.id}, name='{self.name}', exchange='{self.exchange}')"

class Kline(Base):
    """
    Kline (candlestick) data model

    Represents time-series price data for trading pairs.
    This table will be converted to a TimescaleDB hypertable.
    """
    __tablename__ = 'kline_data'

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    symbol_id: Mapped[int] = mapped_column(
        ForeignKey('symbols.id', ondelete='CASCADE'),
        nullable=False,
        index=True
    )
    start_time: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        index=True,
        comment='Start time of the kline in milliseconds'
    )
    timeframe: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        index=True,
        comment='Timeframe of the kline (e.g., "1m", "5m", "1h")'
    )

    # Price and volume fields with explicit precision
    open_price: Mapped[float] = mapped_column(
        Float(precision=18, decimal_return_scale=8),
        nullable=False
    )
    high_price: Mapped[float] = mapped_column(
        Float(precision=18, decimal_return_scale=8),
        nullable=False
    )
    low_price: Mapped[float] = mapped_column(
        Float(precision=18, decimal_return_scale=8),
        nullable=False
    )
    close_price: Mapped[float] = mapped_column(
        Float(precision=18, decimal_return_scale=8),
        nullable=False
    )
    volume: Mapped[float] = mapped_column(
        Float(precision=18, decimal_return_scale=8),
        nullable=False
    )
    turnover: Mapped[float] = mapped_column(
        Float(precision=18, decimal_return_scale=8),
        nullable=False
    )

    # Define the relationship back to Symbol
    symbol: Mapped['Symbol'] = relationship('Symbol', back_populates='klines')

    # Add constraints and indexes for TimescaleDB optimization
    __table_args__ = (
        # Unique constraint for symbol_id + timeframe + start_time combination
        UniqueConstraint(
            'symbol_id', 'timeframe', 'start_time',
            name='uix_kline_symbol_timeframe_start'
        ),
        # Composite index for common query patterns
        Index(
            'idx_kline_query',
            'symbol_id', 'timeframe', 'start_time',
            postgresql_using='btree'
        ),
        # Index for time-based queries
        Index(
            'idx_kline_time',
            'start_time',
            postgresql_using='brin'
        ),
        {'comment': 'Time-series price data for trading pairs'}
    )

    def __repr__(self) -> str:
        return (
            f"Kline(id={self.id}, symbol_id={self.symbol_id}, "
            f"start_time={self.start_time}, timeframe='{self.timeframe}')"
        )

# TimescaleDB conversion commands
create_hypertable = DDL("""
    SELECT create_hypertable(
        'kline_data',
        'start_time',
        chunk_time_interval => INTERVAL '1 week',
        if_not_exists => TRUE
    );
""")

# Add compression policy
add_compression = DDL("""
    ALTER TABLE kline_data SET (
        timescaledb.compress,
        timescaledb.compress_orderby = 'start_time',
        timescaledb.compress_segmentby = 'symbol_id,timeframe'
    );
""")

# Add compression policy (30 days)
add_compression_policy = DDL("""
    SELECT add_compression_policy(
        'kline_data',
        INTERVAL '30 days',
        if_not_exists => TRUE
    );
""")

def get_continuous_aggregate_ddl(default_timeframe: str = '5') -> List[DDL]:
    """Generate DDL statements with configured base timeframe"""
    return [
        DDL("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS kline_1h
        WITH (timescaledb.continuous) AS
        SELECT
        time_bucket('1 hour', to_timestamp(start_time/1000)) AS bucket,
        symbol_id,
        '60' as timeframe,
        first(open_price, start_time) as open_price,
        max(high_price) as high_price,
        min(low_price) as low_price,
        last(close_price, start_time) as close_price,
        sum(volume) as volume,
        sum(turnover) as turnover
        FROM kline_data
        WHERE timeframe = '{default_timeframe}'
        GROUP BY bucket, symbol_id
        WITH NO DATA;

        SELECT add_continuous_aggregate_policy('kline_1h',
        end_offset => INTERVAL '1 hour',
        schedule_interval => INTERVAL '5 minutes');
        """),
        DDL("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS kline_4h
        WITH (timescaledb.continuous) AS
        SELECT
        time_bucket('4 hours', to_timestamp(start_time/1000)) AS bucket,
        symbol_id,
        '240' as timeframe,
        first(open_price, start_time) as open_price,
        max(high_price) as high_price,
        min(low_price) as low_price,
        last(close_price, start_time) as close_price,
        sum(volume) as volume,
        sum(turnover) as turnover
        FROM kline_data
        WHERE timeframe = '{default_timeframe}'
        GROUP BY bucket, symbol_id
        WITH NO DATA;

        SELECT add_continuous_aggregate_policy('kline_4h',
        end_offset => INTERVAL '4 hours',
        schedule_interval => INTERVAL '20 minutes');
        """),
        DDL("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS kline_1d
        WITH (timescaledb.continuous) AS
        SELECT
        time_bucket('1 day', to_timestamp(start_time/1000)) AS bucket,
        symbol_id,
        'D' as timeframe,
        first(open_price, start_time) as open_price,
        max(high_price) as high_price,
        min(low_price) as low_price,
        last(close_price, start_time) as close_price,
        sum(volume) as volume,
        sum(turnover) as turnover
        FROM kline_data
        WHERE timeframe = '{default_timeframe}'
        GROUP BY bucket, symbol_id
        WITH NO DATA;

        SELECT add_continuous_aggregate_policy('kline_1d',
        end_offset => INTERVAL '1 day',
        schedule_interval => INTERVAL '1 hour');
        """)
    ]

# Register the DDL commands to run after table creation
def setup_timescaledb(target, connection, **kw) -> None:
    """Setup TimescaleDB features for the kline_data table"""
    default_timeframe = os.getenv('DEFAULT_TIMEFRAME', '5')
    retention_days = os.getenv('TIMESCALE_RETENTION_DAYS', '0')

    connection.execute(create_hypertable)
    connection.execute(add_compression)
    connection.execute(add_compression_policy)

    if int(retention_days):
        retention_policy = DDL(f"""
            SELECT add_retention_policy(
                'kline_data',
                INTERVAL '{retention_days} days',
                if_not_exists => TRUE
            );
        """)
        connection.execute(retention_policy)

    ddl_statements = get_continuous_aggregate_ddl(default_timeframe)
    for ddl in ddl_statements:
        connection.execute(ddl)

# Listen for table creation event
listen(Kline.__table__, 'after_create', setup_timescaledb)