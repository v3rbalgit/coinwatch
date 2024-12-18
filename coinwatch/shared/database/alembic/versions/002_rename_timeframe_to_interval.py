"""rename timeframe to interval

Revision ID: 002
Revises: 001
Create Date: 2024-01-09

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '002'
down_revision: Union[str, None] = '001'
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

    # Rename column
    op.alter_column('kline_data', 'timeframe', new_column_name='interval', schema='market_data')

    # Update constraint name
    op.drop_constraint('uix_kline_symbol_time', 'kline_data', schema='market_data')
    op.create_unique_constraint('uix_kline_symbol_interval_time', 'kline_data', ['symbol_id', 'interval', 'timestamp'], schema='market_data')

    # Update index
    op.drop_index('idx_kline_query', 'kline_data', schema='market_data')
    op.create_index('idx_kline_query', 'kline_data', ['symbol_id', 'interval', 'timestamp'], postgresql_using='btree', schema='market_data')

    # Re-enable compression with updated settings
    op.execute("""
        ALTER TABLE market_data.kline_data SET (
            timescaledb.compress,
            timescaledb.compress_orderby = 'timestamp',
            timescaledb.compress_segmentby = 'symbol_id,interval'
        );
    """)

    # Re-add compression policy
    op.execute("""
        SELECT add_compression_policy('market_data.kline_data',
            INTERVAL '30 days',
            if_not_exists => TRUE
        );
    """)


def downgrade() -> None:
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

    # Revert index
    op.drop_index('idx_kline_query', 'kline_data', schema='market_data')
    op.create_index('idx_kline_query', 'kline_data', ['symbol_id', 'timeframe', 'timestamp'], postgresql_using='btree', schema='market_data')

    # Revert constraint name
    op.drop_constraint('uix_kline_symbol_interval_time', 'kline_data', schema='market_data')
    op.create_unique_constraint('uix_kline_symbol_time', 'kline_data', ['symbol_id', 'timeframe', 'timestamp'], schema='market_data')

    # Revert column name
    op.alter_column('kline_data', 'interval', new_column_name='timeframe', schema='market_data')

    # Re-enable compression with original settings
    op.execute("""
        ALTER TABLE market_data.kline_data SET (
            timescaledb.compress,
            timescaledb.compress_orderby = 'timestamp',
            timescaledb.compress_segmentby = 'symbol_id,timeframe'
        );
    """)

    # Re-add compression policy
    op.execute("""
        SELECT add_compression_policy('market_data.kline_data',
            INTERVAL '30 days',
            if_not_exists => TRUE
        );
    """)
