"""Make market metrics fields nullable

Revision ID: 009
Revises: 008
Create Date: 2024-12-21 20:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '009'
down_revision: Union[str, None] = '008'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Make fields nullable using batch operations
    with op.batch_alter_table('token_market_metrics', schema='fundamental_data') as batch_op:
        # Price fields
        batch_op.alter_column('current_price',
                            type_=sa.Numeric(20, 10),
                            existing_type=sa.Numeric(20, 10),
                            nullable=True)
        batch_op.alter_column('high_24h',
                            type_=sa.Numeric(20, 10),
                            existing_type=sa.Numeric(20, 10),
                            nullable=True)
        batch_op.alter_column('low_24h',
                            type_=sa.Numeric(20, 10),
                            existing_type=sa.Numeric(20, 10),
                            nullable=True)
        batch_op.alter_column('price_change_24h',
                            type_=sa.Numeric(20, 10),
                            existing_type=sa.Numeric(20, 10),
                            nullable=True)
        batch_op.alter_column('ath',
                            type_=sa.Numeric(20, 10),
                            existing_type=sa.Numeric(20, 10),
                            nullable=True)
        batch_op.alter_column('atl',
                            type_=sa.Numeric(20, 10),
                            existing_type=sa.Numeric(20, 10),
                            nullable=True)

        # Percentage fields
        batch_op.alter_column('price_change_percentage_24h',
                            type_=sa.Numeric(16, 4),
                            existing_type=sa.Numeric(16, 4),
                            nullable=True)
        batch_op.alter_column('market_cap_change_percentage_24h',
                            type_=sa.Numeric(16, 4),
                            existing_type=sa.Numeric(16, 4),
                            nullable=True)
        batch_op.alter_column('ath_change_percentage',
                            type_=sa.Numeric(16, 4),
                            existing_type=sa.Numeric(16, 4),
                            nullable=True)
        batch_op.alter_column('atl_change_percentage',
                            type_=sa.Numeric(16, 4),
                            existing_type=sa.Numeric(16, 4),
                            nullable=True)

        # Large number fields
        batch_op.alter_column('market_cap',
                            type_=sa.Numeric(30, 2),
                            existing_type=sa.Numeric(30, 2),
                            nullable=True)
        batch_op.alter_column('fully_diluted_valuation',
                            type_=sa.Numeric(30, 2),
                            existing_type=sa.Numeric(30, 2),
                            nullable=True)
        batch_op.alter_column('total_volume',
                            type_=sa.Numeric(30, 2),
                            existing_type=sa.Numeric(30, 2),
                            nullable=True)
        batch_op.alter_column('market_cap_change_24h',
                            type_=sa.Numeric(30, 2),
                            existing_type=sa.Numeric(30, 2),
                            nullable=True)

        # Supply fields (max_supply already nullable)
        batch_op.alter_column('circulating_supply',
                            type_=sa.Numeric(20, 8),
                            existing_type=sa.Numeric(20, 8),
                            nullable=True)
        batch_op.alter_column('total_supply',
                            type_=sa.Numeric(20, 8),
                            existing_type=sa.Numeric(20, 8),
                            nullable=True)

        # Other fields
        batch_op.alter_column('market_cap_rank',
                            type_=sa.Integer(),
                            existing_type=sa.Integer(),
                            nullable=True)
        batch_op.alter_column('ath_date',
                            type_=postgresql.TIMESTAMP(timezone=True),
                            existing_type=postgresql.TIMESTAMP(timezone=True),
                            nullable=True)
        batch_op.alter_column('atl_date',
                            type_=postgresql.TIMESTAMP(timezone=True),
                            existing_type=postgresql.TIMESTAMP(timezone=True),
                            nullable=True)


def downgrade() -> None:
    # Make fields non-nullable using batch operations
    with op.batch_alter_table('token_market_metrics', schema='fundamental_data') as batch_op:
        # Price fields
        batch_op.alter_column('current_price',
                            type_=sa.Numeric(20, 10),
                            existing_type=sa.Numeric(20, 10),
                            nullable=False)
        batch_op.alter_column('high_24h',
                            type_=sa.Numeric(20, 10),
                            existing_type=sa.Numeric(20, 10),
                            nullable=False)
        batch_op.alter_column('low_24h',
                            type_=sa.Numeric(20, 10),
                            existing_type=sa.Numeric(20, 10),
                            nullable=False)
        batch_op.alter_column('price_change_24h',
                            type_=sa.Numeric(20, 10),
                            existing_type=sa.Numeric(20, 10),
                            nullable=False)
        batch_op.alter_column('ath',
                            type_=sa.Numeric(20, 10),
                            existing_type=sa.Numeric(20, 10),
                            nullable=False)
        batch_op.alter_column('atl',
                            type_=sa.Numeric(20, 10),
                            existing_type=sa.Numeric(20, 10),
                            nullable=False)

        # Percentage fields
        batch_op.alter_column('price_change_percentage_24h',
                            type_=sa.Numeric(16, 4),
                            existing_type=sa.Numeric(16, 4),
                            nullable=False)
        batch_op.alter_column('market_cap_change_percentage_24h',
                            type_=sa.Numeric(16, 4),
                            existing_type=sa.Numeric(16, 4),
                            nullable=False)
        batch_op.alter_column('ath_change_percentage',
                            type_=sa.Numeric(16, 4),
                            existing_type=sa.Numeric(16, 4),
                            nullable=False)
        batch_op.alter_column('atl_change_percentage',
                            type_=sa.Numeric(16, 4),
                            existing_type=sa.Numeric(16, 4),
                            nullable=False)

        # Large number fields
        batch_op.alter_column('market_cap',
                            type_=sa.Numeric(30, 2),
                            existing_type=sa.Numeric(30, 2),
                            nullable=False)
        batch_op.alter_column('fully_diluted_valuation',
                            type_=sa.Numeric(30, 2),
                            existing_type=sa.Numeric(30, 2),
                            nullable=False)
        batch_op.alter_column('total_volume',
                            type_=sa.Numeric(30, 2),
                            existing_type=sa.Numeric(30, 2),
                            nullable=False)
        batch_op.alter_column('market_cap_change_24h',
                            type_=sa.Numeric(30, 2),
                            existing_type=sa.Numeric(30, 2),
                            nullable=False)

        # Supply fields (max_supply stays nullable)
        batch_op.alter_column('circulating_supply',
                            type_=sa.Numeric(20, 8),
                            existing_type=sa.Numeric(20, 8),
                            nullable=False)
        batch_op.alter_column('total_supply',
                            type_=sa.Numeric(20, 8),
                            existing_type=sa.Numeric(20, 8),
                            nullable=False)

        # Other fields
        batch_op.alter_column('market_cap_rank',
                            type_=sa.Integer(),
                            existing_type=sa.Integer(),
                            nullable=False)
        batch_op.alter_column('ath_date',
                            type_=postgresql.TIMESTAMP(timezone=True),
                            existing_type=postgresql.TIMESTAMP(timezone=True),
                            nullable=False)
        batch_op.alter_column('atl_date',
                            type_=postgresql.TIMESTAMP(timezone=True),
                            existing_type=postgresql.TIMESTAMP(timezone=True),
                            nullable=False)
