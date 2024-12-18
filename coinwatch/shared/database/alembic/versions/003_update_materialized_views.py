"""update materialized views to use interval

Revision ID: 003
Revises: 002
Create Date: 2024-01-09

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '003'
down_revision: Union[str, None] = '002'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop existing views
    op.execute('DROP MATERIALIZED VIEW IF EXISTS market_data.kline_1d CASCADE')
    op.execute('DROP MATERIALIZED VIEW IF EXISTS market_data.kline_4h CASCADE')
    op.execute('DROP MATERIALIZED VIEW IF EXISTS market_data.kline_1h CASCADE')

    # Recreate 1h view
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

    # Recreate 4h view
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

    # Recreate 1d view
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

    # Refresh the views to populate historical data
    op.execute("CALL refresh_continuous_aggregate('market_data.kline_1h', NULL, NULL)")
    op.execute("CALL refresh_continuous_aggregate('market_data.kline_4h', NULL, NULL)")
    op.execute("CALL refresh_continuous_aggregate('market_data.kline_1d', NULL, NULL)")


def downgrade() -> None:
    # Drop views with 'interval'
    op.execute('DROP MATERIALIZED VIEW IF EXISTS market_data.kline_1d CASCADE')
    op.execute('DROP MATERIALIZED VIEW IF EXISTS market_data.kline_4h CASCADE')
    op.execute('DROP MATERIALIZED VIEW IF EXISTS market_data.kline_1h CASCADE')

    # Recreate with 'timeframe'
    op.execute("""
        CREATE MATERIALIZED VIEW market_data.kline_1h
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('1 hour', timestamp, 'UTC') AS bucket,
            symbol_id,
            '60' as timeframe,
            first(open_price, timestamp) as open_price,
            max(high_price) as high_price,
            min(low_price) as low_price,
            last(close_price, timestamp) as close_price,
            sum(volume) as volume,
            sum(turnover) as turnover
        FROM market_data.kline_data
        WHERE timeframe = '5'
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

    op.execute("""
        CREATE MATERIALIZED VIEW market_data.kline_4h
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('4 hours', timestamp, 'UTC') AS bucket,
            symbol_id,
            '240' as timeframe,
            first(open_price, timestamp) as open_price,
            max(high_price) as high_price,
            min(low_price) as low_price,
            last(close_price, timestamp) as close_price,
            sum(volume) as volume,
            sum(turnover) as turnover
        FROM market_data.kline_data
        WHERE timeframe = '5'
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

    op.execute("""
        CREATE MATERIALIZED VIEW market_data.kline_1d
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('1 day', timestamp, 'UTC') AS bucket,
            symbol_id,
            'D' as timeframe,
            first(open_price, timestamp) as open_price,
            max(high_price) as high_price,
            min(low_price) as low_price,
            last(close_price, timestamp) as close_price,
            sum(volume) as volume,
            sum(turnover) as turnover
        FROM market_data.kline_data
        WHERE timeframe = '5'
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

    # Refresh the views to populate historical data
    op.execute("""
        CALL refresh_continuous_aggregate('market_data.kline_1h', NULL, NULL);
        CALL refresh_continuous_aggregate('market_data.kline_4h', NULL, NULL);
        CALL refresh_continuous_aggregate('market_data.kline_1d', NULL, NULL);
    """)
