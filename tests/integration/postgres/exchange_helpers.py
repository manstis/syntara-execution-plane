"""Disposable journal schemas and committed-state probes for protocol tests."""

import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from uuid import uuid4
from weakref import WeakSet

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema


@dataclass
class JournalDatabase:
    """A journal service backed by an isolated schema in a disposable database."""

    store: object
    sessions: async_sessionmaker[AsyncSession]
    live_sessions: WeakSet[AsyncSession]

    async def read_rows(self):
        async with self.sessions() as session:
            return (await session.execute(select(self.store.schema.table))).mappings().all()


@asynccontextmanager
async def journal_database():
    """Own only a newly generated schema; never migrate application databases."""
    from execution_plane_client.crypto import AESGCMCipher
    from execution_plane_transport_journal.schema import build_journal_schema
    from execution_plane_transport_journal.store import ExchangeJournalStore

    url = os.environ.get("EP_TEST_DATABASE_URL")
    if not url:
        pytest.skip("EP_TEST_DATABASE_URL must identify a disposable PostgreSQL database")
    schema_name = "exchange_test_" + uuid4().hex
    engine = create_async_engine(url)
    schema = build_journal_schema(schema_name)
    cipher = AESGCMCipher(active_key_id="test", keys={"test": b"t" * 32})
    try:
        async with engine.begin() as connection:
            await connection.execute(CreateSchema(schema_name))
            await connection.run_sync(schema.metadata.create_all)
        live_sessions = WeakSet()

        class TrackedSession(AsyncSession):
            """Retain live sessions solely to assert external-call boundaries."""

            def __init__(self, *args: object, **kwargs: object) -> None:
                """Track sessions without changing transaction behavior."""
                super().__init__(*args, **kwargs)
                live_sessions.add(self)
                self.owner_task = asyncio.current_task()

        yield JournalDatabase(
            ExchangeJournalStore(schema, cipher),
            async_sessionmaker(engine, class_=TrackedSession, expire_on_commit=False),
            live_sessions,
        )
    finally:
        async with engine.begin() as connection:
            await connection.execute(DropSchema(schema_name, cascade=True, if_exists=True))
        await engine.dispose()


@asynccontextmanager
async def peer_pair(*, options=None, realtime=False):  # noqa: C901, PLR0915 - One fixture composes two real independent participants.
    """Run the shared implementation twice, with independent PostgreSQL schemas."""
    from contextlib import AsyncExitStack
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace

    import httpx
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from fastapi import FastAPI, Request

    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.handlers import CommandRegistry, EffectResult, HandlingResult, PreparedEffect
    from execution_plane_client.protocol import CommandStatus, MessageEnvelope
    from execution_plane_client.security import AssertionSigner, AssertionVerifier, PeerConfig, PeerGrant
    from execution_plane_transport_direct.transport import HttpCommandTransport
    from execution_plane_transport_journal.service import build_journal_service

    async with AsyncExitStack() as stack:
        databases = {name: await stack.enter_async_context(journal_database()) for name in ("ao", "ep")}
        apps = {}
        endpoints = {}
        peers = {}
        private_keys = {}
        pair = SimpleNamespace(
            now=datetime.now(UTC) + timedelta(seconds=1),
            effects=0,
            results=[],
            dropped={},
            blocked=set(),
            executed=set(),
        )
        for name in databases:
            key = ec.generate_private_key(ec.SECP256R1())
            private_keys[name] = key.private_bytes(
                serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
            ).decode()
            public = (
                key.public_key()
                .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
                .decode()
            )
            grant = PeerGrant(
                project_ids=None,
                permissions={
                    "work.execute",
                    "work.execute.completed",
                    "commands:read",
                    "capabilities:read",
                },
            )
            peers[name] = PeerConfig(
                peer_id=name,
                issuer=f"urn:{name}",
                audience=f"urn:{name}",
                role="controller" if name == "ao" else "executor",
                command_url=f"https://{name}.test/v1/commands",
                query_base_url=f"https://{name}.test/v1",
                grants={"ao": grant},
                verification_keys={"k1": public},
                permitted_networks=["192.0.2.0/24"],
            )

        class Network(httpx.AsyncBaseTransport):
            def __init__(self, source, destination) -> None:
                self.source = source
                self.destination = destination

            async def handle_async_request(self, request) -> httpx.Response:
                assert not any(
                    session.in_transaction()
                    for session in databases[self.source].live_sessions
                    if session.owner_task is asyncio.current_task()
                )
                request.headers["x-test-peer"] = self.source
                if request.method == "POST":
                    message = MessageEnvelope.model_validate_json(request.content)
                    if (self.source, message.command.command_type) in pair.blocked:
                        error = "test-only lost callback"
                        raise httpx.ReadError(error)
                response = await httpx.ASGITransport(
                    app=apps[self.destination], raise_app_exceptions=False
                ).handle_async_request(request)
                if request.method == "POST" and pair.dropped.get(self.destination, 0):
                    pair.dropped[self.destination] -= 1
                    await response.aclose()
                    error = "test-only response loss after receiver commit"
                    raise httpx.ReadError(error)
                return response

        class Work:
            async def prepare(self, message, context, session) -> PreparedEffect:
                row = await databases["ep"].store.get(session, message.scope, message.command_id, "inbound")
                return PreparedEffect(row.entry_id, message.command_id, 1)

            async def execute(self, effect) -> EffectResult:
                assert not any(
                    session.in_transaction()
                    for session in databases["ep"].live_sessions
                    if session.owner_task is asyncio.current_task()
                )
                pair.effects += 1
                pair.executed.add(effect.command_id)
                return EffectResult("succeeded", {"value": 42})

            async def finish(self, effect, result, context, session) -> HandlingResult:
                return HandlingResult(status=CommandStatus.SUCCEEDED, result=result.result, completion_required=True)

            async def reconcile(self, effect) -> EffectResult | None:
                return EffectResult("succeeded", {"value": 42}) if effect.command_id in pair.executed else None

        class Actor:
            async def handle(self, message, context, session) -> HandlingResult:
                pair.results.append(message.command_id)
                return HandlingResult()

        async def certificate(request: Request) -> str:
            # Explicit test-only auth injection; no production header bypass.
            return request.headers["x-test-peer"]

        async def configure(name) -> None:
            local = peers[name]
            catalog = CommandCatalog()
            registry = CommandRegistry()
            registry.register(
                catalog.definition("work.execute" if name == "ep" else "work.execute.completed", 1),
                Work() if name == "ep" else Actor(),
            )
            signer = AssertionSigner(
                local_id=name,
                issuer=local.issuer,
                key_id="k1",
                private_key=private_keys[name],
                peers=peers,
                clock=lambda: datetime.now(UTC) if realtime else pair.now,
            )
            verifier = AssertionVerifier(
                local_id=name,
                audience=local.audience,
                command_url=local.command_url,
                peers=peers,
                clock=lambda: datetime.now(UTC) if realtime else pair.now,
            )
            transport = HttpCommandTransport(
                signer=signer, peers=peers, transports={other: Network(name, other) for other in peers if other != name}
            )
            service = build_journal_service(
                store=databases[name].store,
                sessions=databases[name].sessions,
                local_peer=local,
                peers=peers,
                registry=registry,
                transport=transport,
                verifier=verifier,
                certificate_auth=certificate,
                options=options,
                clock=lambda: datetime.now(UTC) if realtime else pair.now,
            )
            app = FastAPI()
            app.include_router(service.create_router())
            apps[name] = app

            async def delivery_state(command_id) -> str:
                from execution_plane_client.protocol import CommandScope

                async with databases[name].sessions() as session:
                    row = await databases[name].store.get(
                        session,
                        CommandScope(client_id="ao", project_id=None, all_projects=True),
                        command_id,
                        "outbound",
                    )
                return row.delivery_state

            endpoints[name] = SimpleNamespace(
                service=service,
                sessions=databases[name].sessions,
                store=databases[name].store,
                delivery_state=delivery_state,
                app=app,
            )
            setattr(pair, name, endpoints[name])

        async def pump(rounds=1) -> None:
            for _ in range(rounds):
                for name in ("ao", "ep"):
                    await endpoints[name].service.delivery.run_once()
                    await endpoints[name].service.handling.run_once()
                for name in ("ao", "ep"):
                    await endpoints[name].service.reconciliation.run_once()
                pair.now += timedelta(seconds=10)

        async def restart(name) -> None:
            await endpoints[name].service.close()
            await configure(name)
            pair.now += timedelta(seconds=60)

        def drop_next_response(name) -> None:
            pair.dropped[name] = pair.dropped.get(name, 0) + 1

        pair.pump, pair.restart, pair.drop_next_response = pump, restart, drop_next_response
        for name in databases:
            await configure(name)
        try:
            yield pair
        finally:
            for endpoint in endpoints.values():
                await endpoint.service.close()
