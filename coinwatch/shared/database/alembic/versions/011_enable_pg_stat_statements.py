"""Enable pg_stat_statements extension

Revision ID: 011
Revises: 010
Create Date: 2024-12-22 00:30:00.000000

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '011'
down_revision: Union[str, None] = '010'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Enable pg_stat_statements extension for query statistics"""
    # Create the extension in the public schema
    op.execute('CREATE EXTENSION IF NOT EXISTS pg_stat_statements SCHEMA public')


def downgrade() -> None:
    """Remove pg_stat_statements extension"""
    op.execute('DROP EXTENSION IF EXISTS pg_stat_statements')
