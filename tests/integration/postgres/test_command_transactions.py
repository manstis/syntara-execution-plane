"""Domain responsibility and mutation share the receipt transaction."""

from uuid import uuid4

import pytest
from sqlalchemy import func, select

from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.models.cluster import Cluster
from execution_plane.models.work_item import WorkItem
from execution_plane.work_store import WorkStore

from .command_helpers import command_database


@pytest.mark.integration
async def test_receipt_intent_and_work_roll_back_together(work_message):
    async with command_database() as db:
        message = work_message
        async with db.sessions() as session:
            await db.journal.append_or_confirm_inbound(session, message)
            await WorkStore.from_transaction(session).dispatch(
                client_id=message.scope.client_id,
                project_id=message.scope.project_id,
                request_id=str(message.command_id),
                work_correlation_id=uuid4(),
                payload={"image": "test"},
            )
            await session.rollback()
        async with db.sessions() as session:
            assert await session.scalar(select(func.count()).select_from(WorkItem)) == 0
            assert await db.journal.get(session, message.scope, message.command_id, "inbound") is None
            assert await session.scalar(select(func.count()).select_from(db.journal.schema.table)) == 0


@pytest.mark.integration
async def test_borrowed_redaction_preserves_credentials_and_legacy_calls_commit():
    async with command_database() as db:
        async with db.sessions.begin() as session:
            cluster = await ClusterStore.from_transaction(session).create("one", "https://k8s.test", "private", uuid4())
            assert cluster.api_key == ""
        async with db.sessions() as session:
            stored = await session.get(Cluster, cluster.id)
            assert stored.api_key == "private"
            other = await ClusterStore.from_session(session).create("two", "https://k8s.test", "private", uuid4())
            await session.rollback()
        async with db.sessions() as session:
            assert await session.get(Cluster, other.id) is not None


@pytest.mark.integration
async def test_nested_duplicate_insert_keeps_outer_transaction(monkeypatch):
    """Exercise the race branch by hiding only the initial lookup."""
    async with command_database() as db:
        values = {
            "client_id": "ao",
            "project_id": uuid4(),
            "request_id": "same",
            "work_correlation_id": uuid4(),
            "payload": {},
        }
        async with db.sessions.begin() as session:
            first = await WorkStore.from_transaction(session).dispatch(**values)
        async with db.sessions.begin() as session:
            execute = session.execute
            calls = 0

            async def racing_execute(statement):  # noqa: ANN202 - Preserve SQLAlchemy result types in race injection.
                nonlocal calls
                calls += 1
                if calls == 1:
                    return await execute(select(WorkItem).where(WorkItem.id == uuid4()))
                return await execute(statement)

            monkeypatch.setattr(session, "execute", racing_execute)
            same = await WorkStore.from_transaction(session).dispatch(**values)
            assert same.id == first.id
            await ClusterStore.from_transaction(session).create("survives", "https://k8s.test", "private", uuid4())
        async with db.sessions() as session:
            assert await session.scalar(select(func.count()).select_from(Cluster)) == 1
