"""Command cutover rejects unresolved legacy responsibility before DDL."""

import importlib
from unittest.mock import patch

import pytest


@pytest.mark.parametrize("counts", [(1, 0), (0, 1)])
def test_cutover_refuses_unresolved_legacy_work_or_events(counts):
    migration = importlib.import_module("execution_plane.migrations.versions.j6e7f8a9b0c1_add_command_exchange")
    with patch.object(migration.op, "get_bind") as bind, patch.object(migration.op, "create_table") as create:
        bind.return_value.execute.return_value.scalar_one.side_effect = counts
        with pytest.raises(RuntimeError, match="unresolved legacy"):
            migration.upgrade()
        create.assert_not_called()


@pytest.mark.integration
async def test_migrated_command_tables_match_runtime_metadata():
    """Compare the full new table shapes against an independently migrated database."""
    import os

    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext
    from sqlalchemy.ext.asyncio import create_async_engine

    from execution_plane_client.journal.schema import build_journal_schema

    url = os.environ.get("EP_MIGRATION_DATABASE_URL")
    if not url:
        pytest.skip("EP_MIGRATION_DATABASE_URL must identify a migrated disposable database")
    journal = build_journal_schema("execution_plane")
    names = {"command_records", "resource_revisions", "command_cancellation_intents", journal.table.name}

    def compare(connection):  # noqa: ANN202 - SQLAlchemy supplies the synchronous connection.
        context = MigrationContext.configure(
            connection,
            opts={
                "include_schemas": True,
                "compare_server_default": True,
                "include_object": lambda obj, name, kind, _reflected, _compare_to: (
                    kind != "table" or (obj.schema == "execution_plane" and name in names)
                ),
            },
        )
        return compare_metadata(context, journal.metadata)

    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            assert await connection.run_sync(compare) == []
    finally:
        await engine.dispose()


@pytest.mark.parametrize("counts", [(1, 0), (0, 1)])
def test_legacy_cleanup_refuses_unjournaled_outcomes_or_pending_bindings(counts):
    migration = importlib.import_module(
        "execution_plane.migrations.versions.412de0fed627_retire_legacy_exchange_tables"
    )
    with (
        patch.object(migration.op, "get_bind") as bind,
        patch.object(migration.op, "drop_table") as drop,
        patch.object(migration.op, "drop_index") as drop_index,
    ):
        bind.return_value.execute.return_value.scalar_one.side_effect = counts
        with pytest.raises(RuntimeError, match="Refusing legacy cleanup"):
            migration.upgrade()
        drop.assert_not_called()
        drop_index.assert_not_called()


@pytest.mark.integration
async def test_legacy_cleanup_downgrade_and_upgrade_preserve_command_schema():
    import os

    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from sqlalchemy import inspect
    from sqlalchemy.ext.asyncio import create_async_engine

    url = os.environ.get("EP_MIGRATION_DATABASE_URL")
    if not url:
        pytest.skip("EP_MIGRATION_DATABASE_URL must identify a disposable migrated database")
    migration = importlib.import_module(
        "execution_plane.migrations.versions.412de0fed627_retire_legacy_exchange_tables"
    )

    def round_trip(connection):  # noqa: ANN202
        from alembic.config import Config
        from alembic.script import ScriptDirectory

        with Operations.context(MigrationContext.configure(connection)):
            # Recreate the projections using their historical migration, rather
            # than importing ORM models that no longer exist at the current head.
            head = ScriptDirectory.from_config(Config("alembic.ini")).get_current_head()
            cutover = ScriptDirectory.from_config(Config("alembic.ini")).get_revision(head).module
            cutover.downgrade()
            migration.downgrade()
            tables = inspect(connection).get_table_names(schema="execution_plane")
            assert {"completion_events", "cluster_bindings", "exchange_journal", "command_records"} <= set(tables)
            migration.upgrade()
            tables = inspect(connection).get_table_names(schema="execution_plane")
            assert "completion_events" not in tables
            assert "cluster_bindings" not in tables
            assert {"exchange_journal", "command_records", "work_items", "clusters"} <= set(tables)
            cutover.upgrade()

    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection, connection.begin():
            await connection.run_sync(round_trip)
    finally:
        await engine.dispose()
