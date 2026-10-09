"""Alembic environment owned by the optional Journal transport package."""

from __future__ import annotations

import asyncio
import os
from logging.config import fileConfig
from typing import TYPE_CHECKING

from alembic import context
from sqlalchemy import Table, pool, text
from sqlalchemy.ext.asyncio import async_engine_from_config

from execution_plane_transport_journal.schema import build_journal_schema

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection
    from sqlalchemy.schema import SchemaItem

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

schema_name = "execution_plane"
journal_schema = build_journal_schema(schema_name)
target_metadata = journal_schema.metadata
version_table = "alembic_version_journal"
database_url = (
    config.get_main_option("sqlalchemy.url")
    or os.environ.get("EP_JOURNAL_DATABASE_URL")
    or os.environ.get("DATABASE_URL")
)
if not database_url:
    msg = "EP_JOURNAL_DATABASE_URL or DATABASE_URL must be set"
    raise RuntimeError(msg)
config.set_main_option("sqlalchemy.url", database_url)


def include_object(
    obj: SchemaItem,
    _name: str | None,
    type_: str,
    _reflected: bool,  # noqa: FBT001 - Alembic supplies this callback argument positionally.
    _compare_to: SchemaItem | None,
) -> bool:
    """Restrict autogenerate to the Journal table in its configured schema."""
    if type_ == "table":
        return isinstance(obj, Table) and obj.schema == schema_name
    return True


def run_migrations_offline() -> None:
    """Generate SQL without connecting to PostgreSQL."""
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        include_object=include_object,
        version_table=version_table,
        version_table_schema=schema_name,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Run only this package's schema revisions."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        include_object=include_object,
        compare_type=True,
        compare_server_default=True,
        version_table=version_table,
        version_table_schema=schema_name,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Create the participant schema and run Journal-only revisions."""
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))
        await connection.commit()
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    """Run revisions against the configured participant database."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
