"""Update supply field precision

Revision ID: 010
Revises: 009
Create Date: 2024-12-21 21:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '010'
down_revision: Union[str, None] = '009'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Update supply fields precision using batch operations
    with op.batch_alter_table('token_market_metrics', schema='fundamental_data') as batch_op:
        # Supply fields
        batch_op.alter_column('circulating_supply',
                            type_=sa.Numeric(30, 8),
                            existing_type=sa.Numeric(20, 8),
                            nullable=True)
        batch_op.alter_column('total_supply',
                            type_=sa.Numeric(30, 8),
                            existing_type=sa.Numeric(20, 8),
                            nullable=True)
        batch_op.alter_column('max_supply',
                            type_=sa.Numeric(30, 8),
                            existing_type=sa.Numeric(20, 8),
                            nullable=True)


def downgrade() -> None:
    # Revert supply fields precision using batch operations
    with op.batch_alter_table('token_market_metrics', schema='fundamental_data') as batch_op:
        # Supply fields
        batch_op.alter_column('circulating_supply',
                            type_=sa.Numeric(20, 8),
                            existing_type=sa.Numeric(30, 8),
                            nullable=True)
        batch_op.alter_column('total_supply',
                            type_=sa.Numeric(20, 8),
                            existing_type=sa.Numeric(30, 8),
                            nullable=True)
        batch_op.alter_column('max_supply',
                            type_=sa.Numeric(20, 8),
                            existing_type=sa.Numeric(30, 8),
                            nullable=True)
