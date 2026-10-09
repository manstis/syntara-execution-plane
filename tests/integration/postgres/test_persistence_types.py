"""Real PostgreSQL round trips for EP's JSON and enum column types.

Set ``EP_TEST_DATABASE_URL`` to a disposable PostgreSQL database to enable these
checks. The role must be able to create/drop scratch tables and write the EP
tables. The tests create and drop uniquely named scratch tables only.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from execution_plane.models.cluster import Cluster
from execution_plane.models.execution_target import ExecutionTarget as _ExecutionTarget  # noqa: F401
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.work_store import WorkStore


@pytest.mark.asyncio
async def test_project_scope_and_status_round_trip_through_postgresql() -> None:
    database_url = os.environ.get("EP_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("EP_TEST_DATABASE_URL must point to a disposable PostgreSQL database")

    engine = create_async_engine(database_url)
    schema_name = f"ep_smoke_{uuid.uuid4().hex}"
    table_name = "types"
    metadata = sa.MetaData()
    table = sa.Table(
        table_name,
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("project_ids", Cluster.__table__.c.project_ids.type),
        sa.Column("status", WorkItem.__table__.c.status.type, nullable=False),
        schema=schema_name,
    )
    project_ids = [uuid.uuid4(), uuid.uuid4()]

    try:
        async with engine.begin() as connection:
            await connection.execute(CreateSchema(schema_name))
            await connection.run_sync(metadata.create_all)
            await connection.execute(
                table.insert(),
                [
                    {"project_ids": None, "status": WorkItemStatus.PENDING},
                    {"project_ids": [], "status": WorkItemStatus.CLAIMED},
                    {"project_ids": project_ids, "status": WorkItemStatus.DISPATCHED},
                ],
            )
            rows = (await connection.execute(sa.select(table).order_by(table.c.id))).mappings().all()
            sql_null = await connection.scalar(
                sa.select(sa.func.count()).select_from(table).where(table.c.project_ids.is_(None))
            )
    finally:
        async with engine.begin() as connection:
            await connection.execute(DropSchema(schema_name, cascade=True, if_exists=True))
        await engine.dispose()

    assert sql_null == 1
    assert rows[0]["project_ids"] is None
    assert rows[1]["project_ids"] == []
    assert rows[2]["project_ids"] == project_ids
    assert rows[0]["status"] is WorkItemStatus.PENDING
    assert rows[1]["status"] is WorkItemStatus.CLAIMED
    assert rows[2]["status"] is WorkItemStatus.DISPATCHED


@pytest.mark.asyncio
async def test_mapped_rows_reload_project_ids_and_work_item_status(migrated_database: str) -> None:
    """Read project UUIDs and a WorkItem enum back through mapped EP rows."""
    database_url = migrated_database
    engine = create_async_engine(database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    integration_id = uuid.uuid4()
    request_id = f"persistence-round-trip-{uuid.uuid4()}"
    project_ids = [uuid.uuid4(), uuid.uuid4()]
    now = datetime.now(UTC)
    binding_key = integration_id
    client_id = "postgres-type-test"
    claim_owner_id = uuid.uuid4()
    work_item_id: uuid.UUID | None = None

    try:
        async with session_factory() as session:
            session.add(
                Cluster(
                    id=integration_id,
                    created_by=uuid.UUID(int=0),
                    updated_by=uuid.UUID(int=0),
                    name="postgres-type-round-trip",
                    endpoint="https://cluster.invalid",
                    api_key="test-credential",
                    project_ids=project_ids,
                    created_at=now,
                    updated_at=now,
                )
            )
            item = WorkItem(
                client_id=client_id,
                project_id=project_ids[0],
                request_id=request_id,
                request_hash="test-hash",
                work_correlation_id=uuid.uuid4(),
                payload={"invocation": {"version": 1}},
                status=WorkItemStatus.CLAIMED,
                claimed_at=now,
                claim_owner_id=claim_owner_id,
                claim_generation=1,
                created_at=now,
            )
            session.add(item)
            await session.commit()
            work_item_id = item.id

        async with session_factory() as session:
            binding = await session.get(Cluster, binding_key)
            restored_item = await session.get(WorkItem, work_item_id)

        assert binding is not None
        assert binding.project_ids == project_ids
        assert restored_item is not None
        assert restored_item.status is WorkItemStatus.CLAIMED

        work_store = WorkStore.from_engine(engine)
        assert await work_store.mark_dispatched(
            work_item_id,
            claim_owner_id=claim_owner_id,
            claim_generation=1,
        )
        dispatched = await work_store.get(
            work_item_id,
            client_id=client_id,
            project_id=project_ids[0],
        )
        assert dispatched is not None
        assert dispatched.status is WorkItemStatus.DISPATCHED

    finally:
        async with session_factory() as session:
            binding = await session.get(Cluster, binding_key)
            if binding is not None:
                await session.delete(binding)
            item = await session.get(WorkItem, work_item_id) if work_item_id is not None else None
            if item is not None:
                await session.delete(item)
            await session.commit()
        await engine.dispose()
