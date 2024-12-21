"""Update platform cascade and market metrics precision

Revision ID: 006
Revises: 005
Create Date: 2024-01-10

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '006'
down_revision: Union[str, None] = '005'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Remove cascade from platform foreign key
    op.drop_constraint(
        'fk_token_platforms_token_id_token_metadata',
        'token_platforms',
        schema='fundamental_data',
        type_='foreignkey'
    )
    op.create_foreign_key(
        'fk_token_platforms_token_id_token_metadata',
        'token_platforms',
        'token_metadata',
        ['token_id'],
        ['id'],
        source_schema='fundamental_data',
        referent_schema='fundamental_data'
    )

    # 2. Update market metrics columns with proper NUMERIC precision
    with op.batch_alter_table('token_market_metrics', schema='fundamental_data') as batch_op:
        # Price fields (high precision for small values)
        batch_op.alter_column('current_price', type_=sa.Numeric(20, 10), nullable=False)
        batch_op.alter_column('high_24h', type_=sa.Numeric(20, 10), nullable=False)
        batch_op.alter_column('low_24h', type_=sa.Numeric(20, 10), nullable=False)
        batch_op.alter_column('price_change_24h', type_=sa.Numeric(20, 10), nullable=False)
        batch_op.alter_column('ath', type_=sa.Numeric(20, 10), nullable=False)
        batch_op.alter_column('atl', type_=sa.Numeric(20, 10), nullable=False)

        # Percentage fields (lower precision)
        batch_op.alter_column('price_change_percentage_24h', type_=sa.Numeric(7, 2), nullable=False)
        batch_op.alter_column('market_cap_change_percentage_24h', type_=sa.Numeric(7, 2), nullable=False)
        batch_op.alter_column('ath_change_percentage', type_=sa.Numeric(7, 2), nullable=False)
        batch_op.alter_column('atl_change_percentage', type_=sa.Numeric(7, 2), nullable=False)

        # Large number fields (high precision, fewer decimals)
        batch_op.alter_column('market_cap', type_=sa.Numeric(30, 2), nullable=False)
        batch_op.alter_column('fully_diluted_valuation', type_=sa.Numeric(30, 2), nullable=False)
        batch_op.alter_column('total_volume', type_=sa.Numeric(30, 2), nullable=False)
        batch_op.alter_column('market_cap_change_24h', type_=sa.Numeric(30, 2), nullable=False)

        # Supply fields (fewer decimals for token amounts)
        batch_op.alter_column('circulating_supply', type_=sa.Numeric(20, 8), nullable=False)
        batch_op.alter_column('total_supply', type_=sa.Numeric(20, 8), nullable=False)
        batch_op.alter_column('max_supply', type_=sa.Numeric(20, 8), nullable=False)

        # Other fields
        batch_op.alter_column('market_cap_rank', type_=sa.Integer(), nullable=False)
        batch_op.alter_column('ath_date',
            type_=sa.TIMESTAMP(timezone=True),
            nullable=False,
            postgresql_using='ath_date::timestamp with time zone'
        )
        batch_op.alter_column('atl_date',
            type_=sa.TIMESTAMP(timezone=True),
            nullable=False,
            postgresql_using='atl_date::timestamp with time zone'
        )
        # updated_at is already timestamptz, no need for USING clause
        batch_op.alter_column('updated_at',
            type_=sa.TIMESTAMP(timezone=True),
            nullable=False
        )


def downgrade() -> None:
    # 1. Restore cascade delete on platform foreign key
    op.drop_constraint(
        'fk_token_platforms_token_id_token_metadata',
        'token_platforms',
        schema='fundamental_data',
        type_='foreignkey'
    )
    op.create_foreign_key(
        'fk_token_platforms_token_id_token_metadata',
        'token_platforms',
        'token_metadata',
        ['token_id'],
        ['id'],
        source_schema='fundamental_data',
        referent_schema='fundamental_data',
        ondelete='CASCADE'
    )

    # 2. Revert market metrics columns to original types
    with op.batch_alter_table('token_market_metrics', schema='fundamental_data') as batch_op:
        # Price fields
        batch_op.alter_column('current_price', type_=sa.Float(), nullable=False)
        batch_op.alter_column('high_24h', type_=sa.Float(), nullable=True)
        batch_op.alter_column('low_24h', type_=sa.Float(), nullable=True)
        batch_op.alter_column('price_change_24h', type_=sa.Float(), nullable=True)
        batch_op.alter_column('ath', type_=sa.Float(), nullable=True)
        batch_op.alter_column('atl', type_=sa.Float(), nullable=True)

        # Percentage fields
        batch_op.alter_column('price_change_percentage_24h', type_=sa.Float(), nullable=True)
        batch_op.alter_column('market_cap_change_percentage_24h', type_=sa.Float(), nullable=True)
        batch_op.alter_column('ath_change_percentage', type_=sa.Float(), nullable=True)
        batch_op.alter_column('atl_change_percentage', type_=sa.Float(), nullable=True)

        # Large number fields
        batch_op.alter_column('market_cap', type_=sa.Float(), nullable=True)
        batch_op.alter_column('fully_diluted_valuation', type_=sa.Float(), nullable=True)
        batch_op.alter_column('total_volume', type_=sa.Float(), nullable=False)
        batch_op.alter_column('market_cap_change_24h', type_=sa.Float(), nullable=True)

        # Supply fields
        batch_op.alter_column('circulating_supply', type_=sa.Float(), nullable=True)
        batch_op.alter_column('total_supply', type_=sa.Float(), nullable=True)
        batch_op.alter_column('max_supply', type_=sa.Float(), nullable=True)

        # Other fields
        batch_op.alter_column('market_cap_rank', type_=sa.Integer(), nullable=True)
        batch_op.alter_column('ath_date', type_=sa.Text(), nullable=True)
        batch_op.alter_column('atl_date', type_=sa.Text(), nullable=True)
        batch_op.alter_column('updated_at', type_=sa.DateTime(), nullable=False)
