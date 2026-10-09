"""Installed EP process for AO's bilateral tests; only this test driver is mounted.

The line protocol injects transport/provider boundaries. Persistence, HTTP app,
assertion verification, domain adapters and journal workers are production code.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
from sqlalchemy import Column, MetaData, Table, Uuid, func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema
from sqlmodel import SQLModel

from execution_plane.api.main import create_app
from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.commands.clusters import ClusterCommandHandler
from execution_plane.commands.control import CancelCommandHandler
from execution_plane.commands.targets import TargetCommandHandler
from execution_plane.commands.work import WorkCommandHandler
from execution_plane.drain_monitor import DrainMonitor
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.models.cluster import Cluster
from execution_plane.models.execution_target import ExecutionTarget
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.work_store import WorkStore
from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.crypto import AESGCMCipher
from execution_plane_client.handlers import CommandRegistry
from execution_plane_client.protocol import CommandScope
from execution_plane_client.security import AssertionVerifier, PeerConfig
from execution_plane_transport_journal.claims import JournalClaims
from execution_plane_transport_journal.completion_recorder import JournalCompletionRecorder
from execution_plane_transport_journal.ep_queries import EPJournalCommandQueryProvider
from execution_plane_transport_journal.queries import JournalSessionCommandQueryProvider
from execution_plane_transport_journal.schema import build_journal_schema
from execution_plane_transport_journal.service import build_journal_service
from execution_plane_transport_journal.store import ExchangeJournalStore


def emit(value):
    sys.stdout.write("@@bilateral@@" + json.dumps(value) + "\n")
    sys.stdout.flush()


async def read():
    return json.loads(await asyncio.to_thread(sys.stdin.readline))


class Transport:
    """Forward sends through the parent transport boundary."""

    def __init__(self) -> None:
        """Keep concurrent worker sends ordered on the single test channel."""
        self.lock = asyncio.Lock()

    async def send(self, message):
        async with self.lock:
            emit({"delivery": message.model_dump(mode="json")})
            response = await read()
            assert response == {"received": True}, response

    async def get_capabilities(self, peer_id, scope):
        # The controller advertises its real installed handlers in the parent.
        from execution_plane_client.protocol import CapabilitiesRead

        async with self.lock:
            emit({"capabilities": peer_id})
            return CapabilitiesRead.model_validate(await read())

    async def aclose(self):
        pass


async def verified_test_certificate(_request):
    return "syntara-orchestration"


class Peer:
    """Control actual EP persistence and handlers across process restarts."""

    async def initialize(self, values):
        self.schema = values["schema"]
        assert self.schema.startswith("bilateral_")
        assert self.schema[10:].isalnum()
        os.environ["EP_DATABASE_URL"] = values["database_url"]
        os.environ["EP_CREDENTIAL_ENCRYPTION_KEY"] = base64.urlsafe_b64encode(b"t" * 32).decode()
        self.engine = create_async_engine(
            values["database_url"], execution_options={"schema_translate_map": {"execution_plane": self.schema}}
        )
        journal_schema = build_journal_schema("execution_plane")
        self.provider_calls = Table(
            "provider_calls", MetaData(), Column("work_id", Uuid, primary_key=True), schema=self.schema
        )
        async with self.engine.begin() as connection:
            await connection.execute(CreateSchema(self.schema, if_not_exists=True))
            await connection.run_sync(SQLModel.metadata.create_all)
            await connection.run_sync(journal_schema.metadata.create_all)
            await connection.run_sync(self.provider_calls.metadata.create_all)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.journal = ExchangeJournalStore(
            journal_schema, AESGCMCipher(active_key_id="test", keys={"test": b"t" * 32})
        )
        peers = {name: PeerConfig.model_validate(value) for name, value in values["peers"].items()}
        local = peers["execution-plane"]
        registry, catalog = CommandRegistry(), CommandCatalog()
        journal_queries = JournalSessionCommandQueryProvider(self.journal)
        command_queries = EPJournalCommandQueryProvider(self.sessions, journal_queries)
        for kind in ("register", "provision", "update", "delete"):
            registry.register(
                catalog.definition(f"cluster.{kind}", 1), ClusterCommandHandler(self.journal.resource_intents)
            )
        for kind in ("register", "update", "delete"):
            registry.register(
                catalog.definition(f"execution_target.{kind}", 1), TargetCommandHandler(self.journal.resource_intents)
            )
        registry.register(catalog.definition("work.execute", 1), WorkCommandHandler())
        registry.register(catalog.definition("command.cancel", 1), CancelCommandHandler(command_queries))
        self.runtime = build_journal_service(
            store=self.journal,
            sessions=self.sessions,
            local_peer=local,
            peers=peers,
            registry=registry,
            transport=Transport(),
            verifier=AssertionVerifier(
                local_id=local.peer_id, audience=local.audience, command_url=local.command_url, peers=peers
            ),
            certificate_auth=verified_test_certificate,
            queries=journal_queries,
        )
        self.app = create_app(runtime=self.runtime)
        self.work = WorkStore.from_engine(self.engine)
        self.work.set_completion_recorder(JournalCompletionRecorder(self.journal))
        return {"ready": True}

    async def receive(self, values):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="https://ep.test") as client:
            response = await client.post(
                "/v1/commands", json=values["message"], headers={"Authorization": "Bearer " + values["assertion"]}
            )
            return {"status": response.status_code, "body": response.json()}

    async def execute(self, values):
        item = await self.work.claim_one()
        if item is not None:
            await self.work.mark_dispatched(
                item.id, claim_owner_id=item.claim_owner_id, claim_generation=item.claim_generation
            )
            # A durable sentinel represents the external provider's independent state.
            async with self.engine.begin() as connection:
                await connection.execute(self.provider_calls.insert().values(work_id=item.id))
            if values.get("unknown"):
                await self.work.mark_reconciliation_required(item.id, "test provider response lost")
            else:
                await self.work.set_result(item.id, {"value": 42}, WorkItemStatus.COMPLETED)
        return await self.snapshot({})

    async def expire(self, values):
        past = datetime.now(UTC) - timedelta(seconds=1)
        async with self.sessions.begin() as session:
            await session.execute(
                update(self.journal.schema.table).values(
                    delivery_due_at=past,
                    handling_due_at=past,
                    delivery_lease_until=past,
                    handling_lease_until=past,
                    reconciliation_due_at=past,
                )
            )
        return {}

    async def claim(self, values):
        async with self.sessions.begin() as session:
            claims = await JournalClaims(self.journal).claim_due_handling(session, uuid4(), datetime.now(UTC), 10)
        return {"claims": len(claims)}

    async def query(self, values):
        scope = CommandScope.model_validate(values["scope"])
        async with self.sessions() as session:
            if values.get("children"):
                result = await self.runtime.queries.children(
                    session,
                    scope,
                    UUID(values["id"]),
                    values.get("after", 0),
                    1,
                    peer_id="syntara-orchestration",
                    include_recovery=True,
                )
            else:
                result = await self.runtime.queries.get(
                    session, scope, UUID(values["id"]), peer_id="syntara-orchestration"
                )
        return result.model_dump(mode="json")

    async def snapshot(self, values):
        async with self.sessions() as session:
            table = self.journal.schema.table
            rows = (await session.execute(select(table))).mappings().all()
            raw = str(rows)
            assert "echo hello" not in raw
            assert "private-test-token" not in raw
            return {
                "provider_calls": await session.scalar(select(func.count()).select_from(self.provider_calls)),
                "work": await session.scalar(select(func.count()).select_from(WorkItem)),
                "clusters": await session.scalar(select(func.count()).select_from(Cluster)),
                "targets": await session.scalar(select(func.count()).select_from(ExecutionTarget)),
                "journal": [
                    {
                        key: str(row[key]) if row[key] is not None else None
                        for key in (
                            "command_id",
                            "command_type",
                            "direction",
                            "delivery_state",
                            "handling_state",
                            "command_status",
                        )
                    }
                    for row in rows
                ],
            }

    async def handle(self, values):
        return {"handled": await self.runtime.handling.run_once()}

    async def deliver(self, values):
        return {"delivered": await self.runtime.delivery.run_once()}

    async def capabilities(self, values):
        return self.runtime.capabilities.advertise().model_dump(mode="json")

    async def observe(self, values):
        # Advance pending observation deadlines for this explicitly driven test peer.
        async with self.sessions.begin() as session:
            table = self.journal.schema.table
            await session.execute(
                update(table)
                .where(table.c.observation_pending)
                .values(handling_due_at=datetime.now(UTC) - timedelta(seconds=1))
            )
        await self.runtime.handling.run_once()
        return {}

    async def read_http(self, values):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="https://ep.test") as client:
            response = await client.get(values["path"], headers={"Authorization": "Bearer " + values["assertion"]})
            return {"status": response.status_code, "body": response.json()}

    async def drain(self, values):
        monitor = DrainMonitor(
            ExecutionTargetStore.from_engine(self.engine), ClusterStore.from_engine(self.engine), self.work
        )
        try:
            await monitor.start()
            await asyncio.wait_for(monitor.wait_for_idle(), 10)
        finally:
            await monitor.stop()
        await self.observe({})
        return await self.snapshot({})

    async def cleanup(self, values):
        await self.runtime.close()
        async with self.engine.begin() as connection:
            await connection.execute(DropSchema(self.schema, cascade=True))
        await self.engine.dispose()
        return {}


async def main():
    peer = Peer()
    while True:
        values = await read()
        result = await getattr(peer, values.pop("operation"))(values)
        emit({"result": result})
        if values.get("exit"):
            break


if __name__ == "__main__":
    asyncio.run(main())
