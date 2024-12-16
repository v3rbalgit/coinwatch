"""Initial schema setup with TimescaleDB

Revision ID: 001
Revises:
Create Date: 2024-02-14

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Create extensions
    op.execute('CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE')

    # Create schemas
    op.execute('CREATE SCHEMA IF NOT EXISTS market_data')
    op.execute('CREATE SCHEMA IF NOT EXISTS fundamental_data')

    # Market Data Schema
    op.create_table(
        'symbols',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('name', sa.Text(), nullable=False),
        sa.Column('exchange', sa.Text(), nullable=False),
        sa.Column('first_trade_time', sa.BigInteger(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name', 'exchange', name='uix_symbol_exchange'),
        sa.Index('idx_symbol_lookup', 'name', 'exchange'),
        schema='market_data'
    )

    op.create_table(
        'kline_data',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.BigInteger(), nullable=False),
        sa.Column('timestamp', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('timeframe', sa.Text(), nullable=False),
        sa.Column('open_price', sa.Float(precision=18), nullable=False),
        sa.Column('high_price', sa.Float(precision=18), nullable=False),
        sa.Column('low_price', sa.Float(precision=18), nullable=False),
        sa.Column('close_price', sa.Float(precision=18), nullable=False),
        sa.Column('volume', sa.Float(precision=18), nullable=False),
        sa.Column('turnover', sa.Float(precision=18), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['market_data.symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id', 'timestamp'),
        sa.UniqueConstraint('symbol_id', 'timeframe', 'timestamp', name='uix_kline_symbol_time'),
        sa.Index('idx_kline_query', 'symbol_id', 'timeframe', 'timestamp', postgresql_using='btree'),
        schema='market_data'
    )

    # Setup TimescaleDB for kline_data
    op.execute("""
        SELECT create_hypertable(
            'market_data.kline_data',
            'timestamp',
            chunk_time_interval => INTERVAL '1 week',
            if_not_exists => TRUE,
            migrate_data => TRUE
        );
    """)

    # Setup compression
    op.execute("""
        ALTER TABLE market_data.kline_data SET (
            timescaledb.compress,
            timescaledb.compress_orderby = 'timestamp',
            timescaledb.compress_segmentby = 'symbol_id,timeframe'
        );
    """)

    op.execute("""
        SELECT add_compression_policy(
            'market_data.kline_data',
            INTERVAL '30 days',
            if_not_exists => TRUE
        );
    """)

    # Setup retention
    op.execute("""
        SELECT add_retention_policy(
            'market_data.kline_data',
            INTERVAL '90 days',
            if_not_exists => TRUE
        );
    """)

    # Create continuous aggregates for different timeframes
    # 1h view
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

    # 4h view
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

    # 1d view
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

    # Fundamental Data Schema
    op.create_table(
        'token_metadata',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('symbol', sa.Text(), nullable=False),
        sa.Column('name', sa.Text(), nullable=False),
        sa.Column('description', sa.Text(), nullable=False),
        sa.Column('categories', postgresql.JSONB(), nullable=False),
        sa.Column('launch_time', sa.DateTime(timezone=True), nullable=True),
        sa.Column('market_cap_rank', sa.BigInteger(), nullable=False),
        sa.Column('hashing_algorithm', sa.Text(), nullable=True),
        sa.Column('image_thumb', sa.Text(), nullable=False),
        sa.Column('image_small', sa.Text(), nullable=False),
        sa.Column('image_large', sa.Text(), nullable=False),
        sa.Column('website', sa.Text(), nullable=True),
        sa.Column('whitepaper', sa.Text(), nullable=True),
        sa.Column('reddit', sa.Text(), nullable=True),
        sa.Column('twitter', sa.Text(), nullable=True),
        sa.Column('telegram', sa.Text(), nullable=True),
        sa.Column('github', sa.Text(), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('data_source', sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        schema='fundamental_data'
    )

    op.create_table(
        'token_platforms',
        sa.Column('token_id', sa.Text(), nullable=False),
        sa.Column('platform_id', sa.Text(), nullable=False),
        sa.Column('contract_address', sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(['token_id'], ['fundamental_data.token_metadata.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('token_id', 'platform_id'),
        schema='fundamental_data'
    )

    op.create_table(
        'token_market_metrics',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('symbol', sa.Text(), nullable=False),
        sa.Column('current_price', sa.Float(precision=18), nullable=False),
        sa.Column('fully_diluted_valuation', sa.Float(), nullable=True),
        sa.Column('total_volume', sa.Float(), nullable=False),
        sa.Column('high_24h', sa.Float(), nullable=True),
        sa.Column('low_24h', sa.Float(), nullable=True),
        sa.Column('price_change_24h', sa.Float(), nullable=True),
        sa.Column('price_change_percentage_24h', sa.Float(), nullable=True),
        sa.Column('market_cap', sa.Float(), nullable=True),
        sa.Column('market_cap_rank', sa.Integer(), nullable=True),
        sa.Column('market_cap_change_24h', sa.Float(), nullable=True),
        sa.Column('market_cap_change_percentage_24h', sa.Float(), nullable=True),
        sa.Column('circulating_supply', sa.Float(), nullable=True),
        sa.Column('total_supply', sa.Float(), nullable=True),
        sa.Column('max_supply', sa.Float(), nullable=True),
        sa.Column('ath', sa.Float(), nullable=True),
        sa.Column('ath_change_percentage', sa.Float(), nullable=True),
        sa.Column('ath_date', sa.Text(), nullable=True),
        sa.Column('atl', sa.Float(), nullable=True),
        sa.Column('atl_change_percentage', sa.Float(), nullable=True),
        sa.Column('atl_date', sa.Text(), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('data_source', sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        schema='fundamental_data'
    )

    op.create_table(
        'token_sentiment',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('symbol', sa.Text(), nullable=False),
        sa.Column('twitter_followers', sa.BigInteger(), nullable=True),
        sa.Column('twitter_following', sa.BigInteger(), nullable=True),
        sa.Column('twitter_posts_24h', sa.BigInteger(), nullable=True),
        sa.Column('twitter_engagement_rate', sa.Numeric(), nullable=True),
        sa.Column('twitter_sentiment_score', sa.Numeric(), nullable=True),
        sa.Column('reddit_subscribers', sa.BigInteger(), nullable=True),
        sa.Column('reddit_active_users', sa.BigInteger(), nullable=True),
        sa.Column('reddit_posts_24h', sa.BigInteger(), nullable=True),
        sa.Column('reddit_comments_24h', sa.BigInteger(), nullable=True),
        sa.Column('reddit_sentiment_score', sa.Numeric(), nullable=True),
        sa.Column('telegram_members', sa.BigInteger(), nullable=True),
        sa.Column('telegram_online_members', sa.BigInteger(), nullable=True),
        sa.Column('telegram_messages_24h', sa.BigInteger(), nullable=True),
        sa.Column('telegram_sentiment_score', sa.Numeric(), nullable=True),
        sa.Column('overall_sentiment_score', sa.Numeric(), nullable=False),
        sa.Column('social_score', sa.Numeric(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('data_source', sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        schema='fundamental_data'
    )

def downgrade() -> None:
    # Drop continuous aggregates
    op.execute('DROP MATERIALIZED VIEW IF EXISTS market_data.kline_1d CASCADE')
    op.execute('DROP MATERIALIZED VIEW IF EXISTS market_data.kline_4h CASCADE')
    op.execute('DROP MATERIALIZED VIEW IF EXISTS market_data.kline_1h CASCADE')

    # Drop fundamental data schema tables
    op.drop_table('token_sentiment', schema='fundamental_data')
    op.drop_table('token_market_metrics', schema='fundamental_data')
    op.drop_table('token_platforms', schema='fundamental_data')
    op.drop_table('token_metadata', schema='fundamental_data')

    # Drop market data schema tables
    op.execute('DROP TABLE market_data.kline_data CASCADE')  # CASCADE to handle TimescaleDB dependencies
    op.drop_table('symbols', schema='market_data')

    # Drop schemas
    op.execute('DROP SCHEMA IF EXISTS market_data CASCADE')
    op.execute('DROP SCHEMA IF EXISTS fundamental_data CASCADE')

    # Note: We don't drop the TimescaleDB extension as it might be used by other databases
