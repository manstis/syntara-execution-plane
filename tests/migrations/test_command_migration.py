"""Keep the optional Journal migration chain outside Execution Plane migrations."""

import importlib
from pathlib import Path
from unittest.mock import patch

import pytest


def test_execution_plane_migrations_do_not_import_transport_packages():
    source = Path("src/execution_plane/migrations/env.py").read_text()

    assert "execution_plane_transport_direct" not in source
    assert "execution_plane_transport_journal" not in source


def test_journal_migration_uses_only_package_owned_table():
    from execution_plane_transport_journal.schema import build_journal_schema

    migration = importlib.import_module("execution_plane_transport_journal.migrations.versions.0001_exchange_journal")
    schema = build_journal_schema("execution_plane")

    assert schema.table.name == "exchange_journal"
    assert set(schema.metadata.tables) == {"execution_plane.exchange_journal"}
    assert set(schema.table.c.keys()).isdisjoint({"accepted_at", "acceptance_due_at"})
    assert migration.revision == "0001_exchange_journal"


def test_legacy_cleanup_drops_obsolete_tables_without_data_preflight():
    migration = importlib.import_module(
        "execution_plane.migrations.versions.412de0fed627_retire_legacy_exchange_tables"
    )
    inspector = type("Inspector", (), {"has_table": lambda _self, _name, schema: schema == "execution_plane"})()
    with (
        patch.object(migration.op, "get_bind"),
        patch.object(migration, "inspect", return_value=inspector),
        patch.object(migration.op, "drop_table") as drop_table,
    ):
        migration.upgrade()

    assert {call.args[0] for call in drop_table.call_args_list} == set(migration._OBSOLETE_TABLES)


@pytest.mark.integration
async def test_migrated_journal_table_matches_package_metadata():
    """Compare the separate optional Journal migration chain with its metadata."""
    import os

    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext
    from sqlalchemy.ext.asyncio import create_async_engine

    from execution_plane_transport_journal.schema import build_journal_schema

    url = os.environ.get("EP_JOURNAL_MIGRATION_DATABASE_URL")
    if not url:
        pytest.skip("EP_JOURNAL_MIGRATION_DATABASE_URL must identify a Journal-migrated database")
    journal = build_journal_schema("execution_plane")

    def compare(connection):  # noqa: ANN202 - SQLAlchemy supplies the synchronous connection.
        context = MigrationContext.configure(
            connection,
            opts={
                "include_schemas": True,
                "compare_server_default": True,
                "include_object": lambda obj, name, kind, _reflected, _compare_to: (
                    kind != "table" or (obj.schema == "execution_plane" and name == "exchange_journal")
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
