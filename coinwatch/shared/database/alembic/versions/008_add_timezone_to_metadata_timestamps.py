"""Add timezone to metadata timestamps

Revision ID: 008
Revises: 007
Create Date: 2024-12-21 19:00:00.000000

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '008'
down_revision: Union[str, None] = '007'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Convert launch_time and updated_at columns to use timezone
    op.execute("""
        ALTER TABLE fundamental_data.token_metadata
        ALTER COLUMN launch_time TYPE TIMESTAMP WITH TIME ZONE,
        ALTER COLUMN updated_at TYPE TIMESTAMP WITH TIME ZONE
    """)


def downgrade() -> None:
    # Convert back to timestamp without timezone
    op.execute("""
        ALTER TABLE fundamental_data.token_metadata
        ALTER COLUMN launch_time TYPE TIMESTAMP WITHOUT TIME ZONE,
        ALTER COLUMN updated_at TYPE TIMESTAMP WITHOUT TIME ZONE
    """)
