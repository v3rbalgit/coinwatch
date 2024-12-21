"""Update market metrics precision

Revision ID: 007
Revises: 006
Create Date: 2024-12-21

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '007'
down_revision: Union[str, None] = '006'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Update market metrics precision and nullability
    with op.batch_alter_table('token_market_metrics', schema='fundamental_data') as batch_op:
        # Update percentage fields to use higher precision
        batch_op.alter_column('price_change_percentage_24h',
                            type_=sa.Numeric(16, 4),
                            existing_type=sa.Numeric(7, 2),
                            existing_nullable=False)

        batch_op.alter_column('market_cap_change_percentage_24h',
                            type_=sa.Numeric(16, 4),
                            existing_type=sa.Numeric(7, 2),
                            existing_nullable=False)

        batch_op.alter_column('ath_change_percentage',
                            type_=sa.Numeric(16, 4),
                            existing_type=sa.Numeric(7, 2),
                            existing_nullable=False)

        batch_op.alter_column('atl_change_percentage',
                            type_=sa.Numeric(16, 4),
                            existing_type=sa.Numeric(7, 2),
                            existing_nullable=False)

        # Make max_supply nullable
        batch_op.alter_column('max_supply',
                            type_=sa.Numeric(20, 8),
                            existing_type=sa.Numeric(20, 8),
                            nullable=True)


def downgrade() -> None:
    # Revert market metrics precision and nullability
    with op.batch_alter_table('token_market_metrics', schema='fundamental_data') as batch_op:
        # Revert percentage fields to original precision
        batch_op.alter_column('price_change_percentage_24h',
                            type_=sa.Numeric(7, 2),
                            existing_type=sa.Numeric(16, 4),
                            existing_nullable=False)

        batch_op.alter_column('market_cap_change_percentage_24h',
                            type_=sa.Numeric(7, 2),
                            existing_type=sa.Numeric(16, 4),
                            existing_nullable=False)

        batch_op.alter_column('ath_change_percentage',
                            type_=sa.Numeric(7, 2),
                            existing_type=sa.Numeric(16, 4),
                            existing_nullable=False)

        batch_op.alter_column('atl_change_percentage',
                            type_=sa.Numeric(7, 2),
                            existing_type=sa.Numeric(16, 4),
                            existing_nullable=False)

        # Make max_supply non-nullable again
        batch_op.alter_column('max_supply',
                            type_=sa.Numeric(20, 8),
                            existing_type=sa.Numeric(20, 8),
                            nullable=False)
