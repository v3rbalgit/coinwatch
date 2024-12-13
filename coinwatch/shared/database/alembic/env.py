import asyncio
import os
from logging.config import fileConfig
from typing import Dict, Any

from sqlalchemy import pool, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context

# Import all models to ensure they're known to SQLAlchemy
from shared.database.models.base import (
    market_data_metadata,
    fundamental_data_metadata
)
from shared.database.models.market_data import Symbol, Kline
from shared.database.models.fundamental_data import (
    TokenPlatform,
    TokenMetadata,
    TokenMarketMetrics,
    TokenSentiment
)

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Combined metadata for all schemas
target_metadata = {
    'market_data': market_data_metadata,
    'fundamental_data': fundamental_data_metadata
}

# Schema names
SCHEMAS = ['market_data', 'fundamental_data']

def include_object(object, name, type_, reflected, compare_to):
    """Filter objects included in autogeneration of migrations"""
    if hasattr(object, 'schema'):
        return object.schema in SCHEMAS
    return True

def process_revision_directives(context, revision, directives):
    """Allow generation of multiple revision files - one per schema"""
    if not directives[0].upgrade_ops:
        return

    script_directory = directives[0].upgrade_ops.ops[0].module_directory

    for schema in SCHEMAS:
        schema_ops = directives[0].upgrade_ops.ops.__class__()
        for op in directives[0].upgrade_ops.ops:
            if hasattr(op, 'schema') and op.schema == schema:
                schema_ops.append(op)

        if len(schema_ops.ops) > 0:
            new_filename = f"{script_directory}_{schema}.py"
            with open(new_filename, 'w') as f:
                f.write(f"""
\"\"\"${schema} schema migrations

Revision ID: {revision}
\"\"\"
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

def upgrade():
    {schema_ops}

def downgrade():
    {schema_ops.reverse_op()}
""")

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata['market_data'],
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        include_object=include_object,
        process_revision_directives=process_revision_directives,
    )

    with context.begin_transaction():
        context.run_migrations()

def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata['market_data'],
        include_schemas=True,
        include_object=include_object,
        process_revision_directives=process_revision_directives,
    )

    with context.begin_transaction():
        # Create schemas if they don't exist
        for schema in SCHEMAS:
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        context.run_migrations()

async def run_async_migrations() -> None:
    """Run migrations in 'online' mode."""
    config_section = config.get_section(config.config_ini_section)
    if config_section is None:
        raise ValueError("Configuration section not found")

    configuration: Dict[str, Any] = dict(config_section)

    # Update configuration with environment variables
    configuration["sqlalchemy.url"] = configuration["sqlalchemy.url"] % {
        "DB_USER": os.getenv("DB_USER", "coinwatch"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD", "coinwatch"),
        "DB_HOST": os.getenv("DB_HOST", "localhost"),
        "DB_PORT": os.getenv("DB_PORT", "5432"),
        "DB_NAME": os.getenv("DB_NAME", "coinwatch"),
    }

    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()

def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    asyncio.run(run_async_migrations())

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
