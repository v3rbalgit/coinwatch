# src/models/market.py

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

# Create continuous aggregates for common timeframes
create_hourly_aggregate = DDL("""
CREATE MATERIALIZED VIEW IF NOT EXISTS kline_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', to_timestamp(start_time/1000)) AS bucket,
    symbol_id,
    timeframe,
    first(open_price, start_time) as open,
    max(high_price) as high,
    min(low_price) as low,
    last(close_price, start_time) as close,
    sum(volume) as volume,
    sum(turnover) as turnover
FROM kline_data
GROUP BY bucket, symbol_id, timeframe
WITH NO DATA;

SELECT add_continuous_aggregate_policy('kline_hourly',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
""")

create_daily_aggregate = DDL("""
CREATE MATERIALIZED VIEW IF NOT EXISTS kline_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', to_timestamp(start_time/1000)) AS bucket,
    symbol_id,
    timeframe,
    first(open_price, start_time) as open,
    max(high_price) as high,
    min(low_price) as low,
    last(close_price, start_time) as close,
    sum(volume) as volume,
    sum(turnover) as turnover
FROM kline_data
GROUP BY bucket, symbol_id, timeframe
WITH NO DATA;

SELECT add_continuous_aggregate_policy('kline_daily',
    start_offset => INTERVAL '3 months',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day');
""")

# Register the DDL commands to run after table creation
def setup_timescaledb(target, connection, **kw) -> None:
    """Setup TimescaleDB features for the kline_data table"""
    connection.execute(create_hypertable)
    connection.execute(add_compression)
    connection.execute(add_compression_policy)
    connection.execute(create_hourly_aggregate)
    connection.execute(create_daily_aggregate)

# Listen for table creation event
listen(Kline.__table__, 'after_create', setup_timescaledb)