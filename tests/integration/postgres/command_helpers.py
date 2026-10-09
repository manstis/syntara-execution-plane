"""EP domain stores in isolated disposable PostgreSQL schemas."""

from __future__ import annotations

import base64
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema
from sqlmodel import SQLModel

from execution_plane.config import get_ep_settings


@asynccontextmanager
async def command_database():
    import execution_plane.models.execution_target
    import execution_plane.models.work_item  # noqa: F401
    from execution_plane_client.crypto import AESGCMCipher
    from execution_plane_transport_journal.schema import build_journal_schema
    from execution_plane_transport_journal.store import ExchangeJournalStore

    url = os.environ.get("EP_TEST_DATABASE_URL")
    if not url:
        pytest.skip("EP_TEST_DATABASE_URL must identify a disposable PostgreSQL database")
    schema_name = "command_test_" + uuid4().hex
    settings = get_ep_settings()
    previous = settings.credential_encryption_key
    settings.credential_encryption_key = base64.urlsafe_b64encode(b"t" * 32).decode()
    engine = create_async_engine(url, execution_options={"schema_translate_map": {"execution_plane": schema_name}})
    journal_schema = build_journal_schema("execution_plane")
    cipher = AESGCMCipher(active_key_id="test", keys={"test": b"t" * 32})
    try:
        async with engine.begin() as connection:
            await connection.execute(CreateSchema(schema_name))
            await connection.run_sync(SQLModel.metadata.create_all)
            await connection.run_sync(journal_schema.metadata.create_all)
        yield SimpleNamespace(
            journal=ExchangeJournalStore(journal_schema, cipher),
            sessions=async_sessionmaker(engine, expire_on_commit=False),
        )
    finally:
        settings.credential_encryption_key = previous
        async with engine.begin() as connection:
            await connection.execute(DropSchema(schema_name, cascade=True, if_exists=True))
        await engine.dispose()


@asynccontextmanager
async def resource_case():  # noqa: C901, PLR0915 - Real participant fixture and scoped observation helpers.
    """Use real receipt/handling workers around EP's resource command adapters."""
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import select

    from execution_plane.commands.clusters import ClusterCommandHandler
    from execution_plane.commands.targets import TargetCommandHandler
    from execution_plane.models.cluster import Cluster
    from execution_plane.models.execution_target import ExecutionTarget
    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.commands import CommandIdentity, CommandRoute, build_command
    from execution_plane_client.handlers import CommandRegistry
    from execution_plane_client.protocol import MessageEnvelope, WireModel
    from execution_plane_client.security import AuthenticatedIdentity
    from execution_plane_transport_journal.completion_recorder import JournalCompletionRecorder
    from execution_plane_transport_journal.ep_queries import EPJournalCommandQueryProvider
    from execution_plane_transport_journal.ingress import CommandReceiptHandler
    from execution_plane_transport_journal.queries import JournalSessionCommandQueryProvider
    from execution_plane_transport_journal.workers.handling import JournalHandlingWorker

    async with command_database() as db:
        catalog = CommandCatalog()
        registry = CommandRegistry()
        queries = EPJournalCommandQueryProvider(db.sessions, JournalSessionCommandQueryProvider(db.journal))
        for command in ("cluster.register", "cluster.provision", "cluster.update", "cluster.delete"):
            registry.register(catalog.definition(command, 1), ClusterCommandHandler(db.journal.resource_intents))
        for command in ("execution_target.register", "execution_target.update", "execution_target.delete"):
            registry.register(catalog.definition(command, 1), TargetCommandHandler(db.journal.resource_intents))
        ingress = CommandReceiptHandler(db.journal, catalog, registry, local_id="ep")
        offset = [2]
        worker = JournalHandlingWorker(
            db.journal,
            db.sessions,
            registry,
            local_id="ep",
            completion_recorder=JournalCompletionRecorder(db.journal),
            clock=lambda: datetime.now(UTC) + timedelta(seconds=offset[0]),
        )

        def message(
            kind="cluster.provision", *, key=None, revision=1, client="ao", **values: object
        ) -> MessageEnvelope:
            if kind in {"cluster.register", "cluster.provision"}:
                defaults = {
                    "name": "cluster",
                    "endpoint": "https://cluster.test",
                    "credential": "private",
                    "project_ids": None,
                }
                if kind == "cluster.provision":
                    defaults["default_target"] = {
                        "name": "default",
                        "backend_type": "vanilla_k8s",
                        "endpoint": "https://cluster.test",
                        "placement": {"type": "kubernetes", "namespace": "nodes"},
                    }
                defaults.update(values)
                values = defaults
            payload = catalog.definition(kind, 1).payload_schema.model_validate(
                {"resource_key": key or uuid4(), "desired_revision": revision, **values}
            )
            assert isinstance(payload, WireModel)
            return build_command(
                kind,
                identity=CommandIdentity(command_id=uuid4(), message_id=uuid4(), occurred_at=datetime.now(UTC)),
                route=CommandRoute(
                    source_id=client,
                    destination_id="ep",
                    scope={"client_id": client, "project_id": None, "all_projects": True},
                    target_url="https://ep.test/v1/commands",
                    callback_url=f"https://{client}.test/v1/commands",
                ),
                payload=payload,
            )

        async def receive(message) -> None:
            async with db.sessions.begin() as session:
                await ingress.receive(
                    message,
                    AuthenticatedIdentity(
                        peer_id=message.source_id,
                        scope=message.scope,
                        scopes=frozenset({message.command.command_type}),
                        role="controller",
                    ),
                    session,
                )

        async def run(message) -> SimpleNamespace:
            await receive(message)
            await worker.run_once()
            return await read(message)

        async def read(message) -> SimpleNamespace:
            async with db.sessions() as session:
                record = await db.journal.get(session, message.scope, message.command_id, "inbound")
                assert record is not None
                return SimpleNamespace(
                    **record.model_dump(), status=record.command_status, error_code=record.command_error_code
                )

        async def observe() -> int:
            offset[0] += 2
            return await worker.run_once()

        async def clusters() -> list[Cluster]:
            async with db.sessions() as session:
                return list((await session.scalars(select(Cluster))).all())

        async def targets(cluster_id) -> list[ExecutionTarget]:
            async with db.sessions() as session:
                return list(
                    (
                        await session.scalars(select(ExecutionTarget).where(ExecutionTarget.cluster_id == cluster_id))
                    ).all()
                )

        yield SimpleNamespace(
            db=db,
            message=message,
            receive=receive,
            run=run,
            read=read,
            observe=observe,
            worker=worker,
            clusters=clusters,
            targets=targets,
            command_queries=queries,
            registry=registry,
            catalog=catalog,
            ingress=ingress,
        )


@asynccontextmanager
async def work_case(message):
    """Accepted work and its completion recorder against real PostgreSQL."""
    from sqlalchemy import func, select

    from execution_plane.commands.control import CancelCommandHandler
    from execution_plane.commands.work import WorkCommandHandler
    from execution_plane.models.work_item import WorkItem
    from execution_plane.work_store import WorkStore
    from execution_plane_transport_journal.completion_recorder import JournalCompletionRecorder

    async with resource_case() as case:
        case.registry.register(case.catalog.definition("work.execute", 1), WorkCommandHandler())
        case.registry.register(case.catalog.definition("command.cancel", 1), CancelCommandHandler(case.command_queries))
        await case.run(case.message())
        record = await case.run(message)
        store = WorkStore.from_engine(case.db.sessions.kw["bind"])
        store.set_completion_recorder(JournalCompletionRecorder(case.db.journal))

        async def reload() -> WorkItem:
            async with case.db.sessions() as session:
                return await session.get(WorkItem, record.resource_id)

        async def counts() -> int:
            async with case.db.sessions() as session:
                table = case.db.journal.schema.table
                return await session.scalar(
                    select(func.count()).select_from(table).where(table.c.command_type == "work.execute.completed")
                )

        case.store, case.work_id, case.reload, case.counts = store, record.resource_id, reload, counts
        yield case
