"""Real PostgreSQL cutover preserves projections or rejects the transaction."""

import os
from uuid import uuid4

import pytest
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import create_async_engine


def _migration():  # noqa: ANN202
    scripts = ScriptDirectory.from_config(Config("alembic.ini"))
    head = scripts.get_revision(scripts.get_current_head())
    assert head.down_revision == "ab600379be3e", "single-journal cutover migration is required"
    return head.module


async def _seed(connection):  # noqa: ANN202
    """Build historical rows without depending on retired ORM classes."""
    identities = {name: uuid4() for name in ("project", "cluster", "target", "cancel", "missing", "physical")}
    for name, kind, status in (
        ("cluster", "cluster.delete", "outcome_unknown"),
        ("target", "execution_target.delete", "running"),
        ("cancel", "command.cancel", "running"),
    ):
        parameters = {
            **identities,
            "command": identities[name],
            "type": kind,
            "status": status,
            "entry": uuid4(),
            "message": uuid4(),
            "owner": uuid4(),
        }
        await connection.execute(
            text("""
            INSERT INTO execution_plane.exchange_journal
            (entry_id, message_id, command_id, message_type, command_type, command_revision, sequence,
             source_id, destination_id, client_id, project_id, all_projects, direction, target_url,
             callback_url, envelope_hash, envelope_ciphertext, delivery_state, handling_state,
             command_status, handling_owner, handling_generation, handling_lease_until, created_at, updated_at)
            VALUES (:entry, :message, :command, 'command.submit', :type, 1, 0, 'ao', 'ep', 'ao', :project,
                    false, 'inbound', 'https://ep.test', 'https://ao.test', repeat('a',64),
                    'private-immutable-envelope', 'acknowledged', 'handled', 'accepted', :owner, 5,
                    now() + interval '1 hour', now(), now())
        """),
            parameters,
        )
        await connection.execute(
            text("""
            INSERT INTO execution_plane.command_records
            (client_id, command_id, project_id, all_projects, source_id, command_type, status,
             resource_type, resource_key, resource_id, desired_revision, observed_revision,
             error_code, child_sequence, created_at, updated_at)
            VALUES ('ao', :command, :project, false, 'ao', :type, :status, :resource_type, :key,
                    :resource, :revision, :observed, :error, 0, now(), now())
        """),
            {
                **parameters,
                "resource_type": kind.split(".")[0] if name != "cancel" else None,
                "key": name if name != "cancel" else None,
                "resource": identities["physical"] if name == "target" else None,
                "revision": 2 if name != "cancel" else None,
                "observed": 2 if name == "cluster" else None,
                "error": "unknown-delete" if name == "cluster" else None,
            },
        )
    for name in ("cluster", "target"):
        await connection.execute(
            text("""
            INSERT INTO execution_plane.resource_revisions
            (client_id, resource_type, resource_key, project_id, all_projects, desired_revision,
             observed_revision, desired_hash, resource_id, deleted)
            VALUES ('ao', :kind, :key, :project, false, 2, :observed, repeat('b',64), :resource, :deleted)
        """),
            {
                **identities,
                "kind": "cluster" if name == "cluster" else "execution_target",
                "key": name,
                "observed": 2 if name == "cluster" else 1,
                "resource": identities["physical"] if name == "target" else None,
                "deleted": name == "cluster",
            },
        )
    await connection.execute(
        text("""
        INSERT INTO execution_plane.command_cancellation_intents
        (client_id, scope_key, target_command_id, project_id, all_projects, cancellation_command_id)
        VALUES ('ao', :scope_key, :missing, :project, false, :cancel)
    """),
        {**identities, "scope_key": str(identities["project"])},
    )
    return identities


@pytest.fixture
async def migration_connection():
    url = os.environ.get("EP_MIGRATION_DATABASE_URL")
    if not url:
        pytest.skip("EP_MIGRATION_DATABASE_URL must identify an empty disposable database migrated to head")
    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                yield connection
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()


@pytest.mark.integration
async def test_cutover_transfers_tombstone_pending_observation_and_missing_target_cancellation(migration_connection):
    connection = migration_connection
    migration = _migration()
    await connection.run_sync(lambda conn: _operate(conn, migration.downgrade))
    identities = await _seed(connection)
    await connection.run_sync(lambda conn: _operate(conn, migration.upgrade))
    tables = await connection.run_sync(lambda conn: inspect(conn).get_table_names(schema="execution_plane"))
    assert not {"command_records", "resource_revisions", "command_cancellation_intents"} & set(tables)
    rows = (await connection.execute(text("SELECT * FROM execution_plane.exchange_journal"))).mappings().all()
    by_command = {row["command_id"]: row for row in rows}
    tombstone = by_command[identities["cluster"]]
    assert tombstone["resource_head"]
    assert tombstone["resource_deleted"]
    assert tombstone["resource_id"] is None
    assert tombstone["desired_hash"] == "b" * 64
    assert tombstone["observed_revision"] == 2
    assert tombstone["command_status"] == "outcome_unknown"
    assert tombstone["command_error_code"] == "unknown-delete"
    target = by_command[identities["target"]]
    assert target["resource_id"] == identities["physical"]
    assert target["observed_revision"] == 1
    assert by_command[identities["cancel"]]["target_command_id"] == identities["missing"]
    for row in rows:
        assert row["observation_pending"]
        assert row["handling_state"] == "deferred"
        assert row["handling_due_at"] is not None
        assert row["handling_owner"] is None
        assert row["handling_lease_until"] is None
        assert row["handling_generation"] == 6
        assert row["project_id"] == identities["project"]
        assert not row["all_projects"]
        assert row["envelope_ciphertext"] == "private-immutable-envelope"
    with pytest.raises(RuntimeError, match="Refusing single-journal downgrade"):
        await connection.run_sync(lambda conn: _operate(conn, migration.downgrade))


def _operate(connection, operation) -> None:
    with Operations.context(MigrationContext.configure(connection)):
        operation()


@pytest.mark.integration
@pytest.mark.parametrize("invalid", ["orphan", "resource", "cancel", "scope", "sequence"])
async def test_cutover_refuses_unrepresentable_responsibility_without_losing_tables(migration_connection, invalid):
    connection = migration_connection
    migration = _migration()
    await connection.run_sync(lambda conn: _operate(conn, migration.downgrade))
    identities = await _seed(connection)
    changes = {
        "orphan": "DELETE FROM execution_plane.exchange_journal WHERE command_id = :target",
        "resource": "UPDATE execution_plane.resource_revisions SET desired_revision = 3",
        "cancel": "UPDATE execution_plane.command_cancellation_intents SET cancellation_command_id = :missing",
        "scope": "UPDATE execution_plane.command_records SET project_id = :missing WHERE command_id = :target",
        "sequence": "UPDATE execution_plane.command_records SET child_sequence = 4 WHERE command_id = :target",
    }
    await connection.execute(text(changes[invalid]), identities)
    with pytest.raises(RuntimeError, match="Refusing single-journal cutover") as caught:
        await connection.run_sync(lambda conn: _operate(conn, migration.upgrade))
    assert "private-immutable-envelope" not in str(caught.value)
    tables = await connection.run_sync(lambda conn: inspect(conn).get_table_names(schema="execution_plane"))
    assert {"command_records", "resource_revisions", "command_cancellation_intents"} <= set(tables)
    assert await connection.scalar(text("SELECT count(*) FROM execution_plane.command_records")) == 3


@pytest.mark.integration
@pytest.mark.parametrize("name", ["cluster", "cancel"])
async def test_cutover_preserves_identical_revision_and_duplicate_cancellation_commands(migration_connection, name):
    connection = migration_connection
    migration = _migration()
    await connection.run_sync(lambda conn: _operate(conn, migration.downgrade))
    identities = await _seed(connection)
    duplicate = uuid4()
    parameters = {
        "original": identities[name],
        "duplicate": str(duplicate),
        "entry": str(uuid4()),
        "message": str(uuid4()),
    }
    await connection.execute(
        text("""
        INSERT INTO execution_plane.exchange_journal
        SELECT (jsonb_populate_record(NULL::execution_plane.exchange_journal,
            to_jsonb(j) || jsonb_build_object('entry_id', CAST(:entry AS text),
                'message_id', CAST(:message AS text), 'command_id', CAST(:duplicate AS text)))).*
        FROM execution_plane.exchange_journal j WHERE j.command_id = :original
    """),
        parameters,
    )
    await connection.execute(
        text("""
        INSERT INTO execution_plane.command_records
        SELECT (jsonb_populate_record(NULL::execution_plane.command_records,
            to_jsonb(c) || jsonb_build_object('command_id', CAST(:duplicate AS text),
                'created_at', now() + interval '1 hour'))).*
        FROM execution_plane.command_records c WHERE c.command_id = :original
    """),
        parameters,
    )
    await connection.run_sync(lambda conn: _operate(conn, migration.upgrade))
    row = (
        (
            await connection.execute(
                text("""
        SELECT * FROM execution_plane.exchange_journal WHERE command_id = :command
    """),
                {"command": duplicate},
            )
        )
        .mappings()
        .one()
    )
    assert row["observation_pending"]
    if name == "cluster":
        assert row["resource_head"]
        assert row["resource_deleted"]
        assert (
            await connection.scalar(
                text("""
            SELECT count(*) FROM execution_plane.exchange_journal WHERE resource_key = 'cluster' AND resource_head
        """)
            )
            == 1
        )
    else:
        # The canonical first intent retains the cancellation guard; later
        # accepted commands still carry their encrypted intent for observation.
        assert row["target_command_id"] is None
        assert (
            await connection.scalar(
                text("""
            SELECT count(*) FROM execution_plane.exchange_journal WHERE target_command_id = :missing
        """),
                identities,
            )
            == 1
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    ("status", "association", "expected"),
    [
        ("running", "correct", "cancel_requested"),
        ("accepted", "correct", "cancel_requested"),
        ("cancel_requested", "correct", "cancel_requested"),
        ("failed", "correct", "failed"),
        ("running", "client", "running"),
        ("running", "project", "running"),
        ("running", "request", "running"),
    ],
)
async def test_cutover_preserves_valid_work_cancellation_request(migration_connection, status, association, expected):
    connection = migration_connection
    migration = _migration()
    await connection.run_sync(lambda conn: _operate(conn, migration.downgrade))
    identities = await _seed(connection)
    command_id, work_id = uuid4(), uuid4()
    parameters = {
        **identities,
        "command": str(command_id),
        "work": str(work_id),
        "status": status,
        "entry": str(uuid4()),
        "message": str(uuid4()),
    }
    await connection.execute(
        text("""
        INSERT INTO execution_plane.exchange_journal
        SELECT (jsonb_populate_record(NULL::execution_plane.exchange_journal,
            to_jsonb(j) || jsonb_build_object('entry_id', CAST(:entry AS text),
                'message_id', CAST(:message AS text), 'command_id', CAST(:command AS text),
                'command_type', 'work.execute'))).*
        FROM execution_plane.exchange_journal j WHERE j.command_id = :cancel
    """),
        parameters,
    )
    await connection.execute(
        text("""
        INSERT INTO execution_plane.command_records
        SELECT (jsonb_populate_record(NULL::execution_plane.command_records,
            to_jsonb(c) || jsonb_build_object('command_id', CAST(:command AS text),
                'command_type', 'work.execute', 'resource_type', 'work_item',
                'resource_id', CAST(:work AS text), 'status', CAST(:status AS text)))).*
        FROM execution_plane.command_records c WHERE c.command_id = :cancel
    """),
        parameters,
    )
    await connection.execute(
        text("""
        INSERT INTO execution_plane.work_items
        (id, client_id, project_id, request_id, request_hash, work_correlation_id, status, payload,
         created_at, claim_generation, resource_cleanup_status)
        VALUES (:work_id, :client, :work_project, :request, repeat('a',64), :correlation,
                'cancel_requested', '{}', now(), 0, 'not_required')
    """),
        {
            "work_id": work_id,
            "client": "other" if association == "client" else "ao",
            "work_project": uuid4() if association == "project" else identities["project"],
            "request": str(uuid4()) if association == "request" else str(command_id),
            "correlation": uuid4(),
        },
    )
    await connection.run_sync(lambda conn: _operate(conn, migration.upgrade))
    assert (
        await connection.scalar(
            text("""
        SELECT command_status FROM execution_plane.exchange_journal WHERE command_id = :command_id
    """),
            {"command_id": command_id},
        )
        == expected
    )
