"""update symbol fields

Revision ID: 004
Revises: 003
Create Date: 2024-12-18

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '004'
down_revision: Union[str, None] = '003'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add new columns
    op.add_column('symbols', sa.Column('base_asset', sa.Text(), nullable=True), schema='market_data')
    op.add_column('symbols', sa.Column('quote_asset', sa.Text(), nullable=True), schema='market_data')
    op.add_column('symbols', sa.Column('price_scale', sa.Integer(), nullable=True), schema='market_data')
    op.add_column('symbols', sa.Column('tick_size', sa.Text(), nullable=True), schema='market_data')
    op.add_column('symbols', sa.Column('qty_step', sa.Text(), nullable=True), schema='market_data')
    op.add_column('symbols', sa.Column('max_qty', sa.Text(), nullable=True), schema='market_data')
    op.add_column('symbols', sa.Column('min_notional', sa.Text(), nullable=True), schema='market_data')
    op.add_column('symbols', sa.Column('max_leverage', sa.Text(), nullable=True), schema='market_data')
    op.add_column('symbols', sa.Column('funding_interval', sa.Integer(), nullable=True), schema='market_data')

    # Rename first_trade_time to launch_time to match domain model
    op.alter_column('symbols', 'first_trade_time', new_column_name='launch_time', schema='market_data')


def downgrade() -> None:
    # Rename launch_time back to first_trade_time
    op.alter_column('symbols', 'launch_time', new_column_name='first_trade_time', schema='market_data')

    # Drop added columns
    op.drop_column('symbols', 'funding_interval', schema='market_data')
    op.drop_column('symbols', 'max_leverage', schema='market_data')
    op.drop_column('symbols', 'min_notional', schema='market_data')
    op.drop_column('symbols', 'max_qty', schema='market_data')
    op.drop_column('symbols', 'qty_step', schema='market_data')
    op.drop_column('symbols', 'tick_size', schema='market_data')
    op.drop_column('symbols', 'price_scale', schema='market_data')
    op.drop_column('symbols', 'quote_asset', schema='market_data')
    op.drop_column('symbols', 'base_asset', schema='market_data')
