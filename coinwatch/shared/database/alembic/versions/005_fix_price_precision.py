"""Fix price precision by changing Float to Numeric

Revision ID: 005
Revises: 004
Create Date: 2024-02-19

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '005'
down_revision: str = '004'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # First remove compression policy
    op.execute("""
        SELECT remove_compression_policy('market_data.kline_data', if_exists => true);
    """)

    # Decompress all chunks
    op.execute("""
        SELECT decompress_chunk(i) FROM show_chunks('market_data.kline_data') i;
    """)

    # Now we can disable compression
    op.execute("""
        ALTER TABLE market_data.kline_data SET (timescaledb.compress = false);
    """)
    # Drop existing continuous aggregates
    op.execute('DROP MATERIALIZED VIEW IF EXISTS market_data.kline_1d CASCADE')
    op.execute('DROP MATERIALIZED VIEW IF EXISTS market_data.kline_4h CASCADE')
    op.execute('DROP MATERIALIZED VIEW IF EXISTS market_data.kline_1h CASCADE')


    # Alter column types in kline_data
    # Prices use (18,8) for high precision with 8 decimal places
    # Volume/turnover use (36,8) to handle large numbers while keeping 8 decimal places
    op.alter_column('kline_data', 'open_price',
                    type_=sa.Numeric(precision=18, scale=8),
                    schema='market_data')
    op.alter_column('kline_data', 'high_price',
                    type_=sa.Numeric(precision=18, scale=8),
                    schema='market_data')
    op.alter_column('kline_data', 'low_price',
                    type_=sa.Numeric(precision=18, scale=8),
                    schema='market_data')
    op.alter_column('kline_data', 'close_price',
                    type_=sa.Numeric(precision=18, scale=8),
                    schema='market_data')
    op.alter_column('kline_data', 'volume',
                    type_=sa.Numeric(precision=36, scale=8),
                    schema='market_data')
    op.alter_column('kline_data', 'turnover',
                    type_=sa.Numeric(precision=36, scale=8),
                    schema='market_data')

    # Recreate continuous aggregates with numeric types
    # 1h view
    op.execute("""
        CREATE MATERIALIZED VIEW market_data.kline_1h
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('1 hour', timestamp, 'UTC') AS bucket,
            symbol_id,
            '60' as interval,
            first(open_price, timestamp) as open_price,
            max(high_price) as high_price,
            min(low_price) as low_price,
            last(close_price, timestamp) as close_price,
            sum(volume) as volume,
            sum(turnover) as turnover
        FROM market_data.kline_data
        WHERE interval = '5'
        GROUP BY bucket, symbol_id
        WITH NO DATA;
    """)

    op.execute("""
        SELECT add_continuous_aggregate_policy('market_data.kline_1h',
            start_offset => INTERVAL '3 hours',
            end_offset => INTERVAL '1 hour',
            schedule_interval => INTERVAL '5 minutes',
            if_not_exists => TRUE
        );
    """)

    # 4h view
    op.execute("""
        CREATE MATERIALIZED VIEW market_data.kline_4h
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('4 hours', timestamp, 'UTC') AS bucket,
            symbol_id,
            '240' as interval,
            first(open_price, timestamp) as open_price,
            max(high_price) as high_price,
            min(low_price) as low_price,
            last(close_price, timestamp) as close_price,
            sum(volume) as volume,
            sum(turnover) as turnover
        FROM market_data.kline_data
        WHERE interval = '5'
        GROUP BY bucket, symbol_id
        WITH NO DATA;
    """)

    op.execute("""
        SELECT add_continuous_aggregate_policy('market_data.kline_4h',
            start_offset => INTERVAL '12 hours',
            end_offset => INTERVAL '4 hours',
            schedule_interval => INTERVAL '20 minutes',
            if_not_exists => TRUE
        );
    """)

    # 1d view
    op.execute("""
        CREATE MATERIALIZED VIEW market_data.kline_1d
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('1 day', timestamp, 'UTC') AS bucket,
            symbol_id,
            'D' as interval,
            first(open_price, timestamp) as open_price,
            max(high_price) as high_price,
            min(low_price) as low_price,
            last(close_price, timestamp) as close_price,
            sum(volume) as volume,
            sum(turnover) as turnover
        FROM market_data.kline_data
        WHERE interval = '5'
        GROUP BY bucket, symbol_id
        WITH NO DATA;
    """)

    op.execute("""
        SELECT add_continuous_aggregate_policy('market_data.kline_1d',
            start_offset => INTERVAL '3 days',
            end_offset => INTERVAL '1 day',
            schedule_interval => INTERVAL '1 hour',
            if_not_exists => TRUE
        );
    """)

    # Re-enable compression
    op.execute("""
        ALTER TABLE market_data.kline_data SET (
            timescaledb.compress,
            timescaledb.compress_orderby = 'timestamp',
            timescaledb.compress_segmentby = 'symbol_id,interval'
        );
    """)

    op.execute("""
        SELECT add_compression_policy(
            'market_data.kline_data',
            INTERVAL '30 days',
            if_not_exists => TRUE
        );
    """)

def downgrade() -> None:
    # First disable compression
    op.execute('ALTER TABLE market_data.kline_data SET (timescaledb.compress = false);')
    op.execute('SELECT remove_compression_policy(\'market_data.kline_data\');')

    # Drop continuous aggregates
    op.execute('DROP MATERIALIZED VIEW IF EXISTS market_data.kline_1d CASCADE')
    op.execute('DROP MATERIALIZED VIEW IF EXISTS market_data.kline_4h CASCADE')
    op.execute('DROP MATERIALIZED VIEW IF EXISTS market_data.kline_1h CASCADE')

    # Revert column types in kline_data
    op.alter_column('kline_data', 'open_price',
                    type_=sa.Float(precision=18),
                    schema='market_data')
    op.alter_column('kline_data', 'high_price',
                    type_=sa.Float(precision=18),
                    schema='market_data')
    op.alter_column('kline_data', 'low_price',
                    type_=sa.Float(precision=18),
                    schema='market_data')
    op.alter_column('kline_data', 'close_price',
                    type_=sa.Float(precision=18),
                    schema='market_data')
    op.alter_column('kline_data', 'volume',
                    type_=sa.Float(precision=18),
                    schema='market_data')
    op.alter_column('kline_data', 'turnover',
                    type_=sa.Float(precision=18),
                    schema='market_data')

    # Recreate continuous aggregates with float types
    # 1h view
    op.execute("""
        CREATE MATERIALIZED VIEW market_data.kline_1h
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('1 hour', timestamp, 'UTC') AS bucket,
            symbol_id,
            '60' as interval,
            first(open_price, timestamp) as open_price,
            max(high_price) as high_price,
            min(low_price) as low_price,
            last(close_price, timestamp) as close_price,
            sum(volume) as volume,
            sum(turnover) as turnover
        FROM market_data.kline_data
        WHERE interval = '5'
        GROUP BY bucket, symbol_id
        WITH NO DATA;
    """)

    op.execute("""
        SELECT add_continuous_aggregate_policy('market_data.kline_1h',
            start_offset => INTERVAL '3 hours',
            end_offset => INTERVAL '1 hour',
            schedule_interval => INTERVAL '5 minutes',
            if_not_exists => TRUE
        );
    """)

    # 4h view
    op.execute("""
        CREATE MATERIALIZED VIEW market_data.kline_4h
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('4 hours', timestamp, 'UTC') AS bucket,
            symbol_id,
            '240' as interval,
            first(open_price, timestamp) as open_price,
            max(high_price) as high_price,
            min(low_price) as low_price,
            last(close_price, timestamp) as close_price,
            sum(volume) as volume,
            sum(turnover) as turnover
        FROM market_data.kline_data
        WHERE interval = '5'
        GROUP BY bucket, symbol_id
        WITH NO DATA;
    """)

    op.execute("""
        SELECT add_continuous_aggregate_policy('market_data.kline_4h',
            start_offset => INTERVAL '12 hours',
            end_offset => INTERVAL '4 hours',
            schedule_interval => INTERVAL '20 minutes',
            if_not_exists => TRUE
        );
    """)

    # 1d view
    op.execute("""
        CREATE MATERIALIZED VIEW market_data.kline_1d
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('1 day', timestamp, 'UTC') AS bucket,
            symbol_id,
            'D' as interval,
            first(open_price, timestamp) as open_price,
            max(high_price) as high_price,
            min(low_price) as low_price,
            last(close_price, timestamp) as close_price,
            sum(volume) as volume,
            sum(turnover) as turnover
        FROM market_data.kline_data
        WHERE interval = '5'
        GROUP BY bucket, symbol_id
        WITH NO DATA;
    """)

    op.execute("""
        SELECT add_continuous_aggregate_policy('market_data.kline_1d',
            start_offset => INTERVAL '3 days',
            end_offset => INTERVAL '1 day',
            schedule_interval => INTERVAL '1 hour',
            if_not_exists => TRUE
        );
    """)

    # Re-enable compression
    op.execute("""
        ALTER TABLE market_data.kline_data SET (
            timescaledb.compress,
            timescaledb.compress_orderby = 'timestamp',
            timescaledb.compress_segmentby = 'symbol_id,interval'
        );
    """)

    op.execute("""
        SELECT add_compression_policy(
            'market_data.kline_data',
            INTERVAL '30 days',
            if_not_exists => TRUE
        );
    """)
