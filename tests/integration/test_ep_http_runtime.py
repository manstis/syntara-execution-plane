"""Run EP work ingress and callback delivery over direct HTTP without Journal tables."""

from __future__ import annotations

import asyncio
import base64
import os
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi import FastAPI, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema
from sqlmodel import SQLModel

from execution_plane.commands.work import WorkCommandHandler
from execution_plane.models.work_item import WorkItemStatus
from execution_plane.work_store import WorkStore
from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.commands.work import NodeInvocation, WorkExecute
from execution_plane_client.handlers import CommandRegistry, HandlingResult
from execution_plane_client.protocol import CommandScope, CommandStatus, MessageEnvelope
from execution_plane_client.security import AssertionSigner, AssertionVerifier, PeerConfig, PeerGrant
from execution_plane_transport_direct.completion_recorder import DirectCompletionRecorder
from execution_plane_transport_direct.ep_queries import EPDirectCommandQueryProvider
from execution_plane_transport_direct.service import build_direct_service
from execution_plane_transport_direct.transport import HttpCommandTransport


@asynccontextmanager
async def ep_domain_database():
    """Create only EP's domain tables, proving direct work needs no Journal schema."""
    import execution_plane.models.cluster
    import execution_plane.models.execution_target  # noqa: F401

    url = os.environ.get("EP_TEST_DATABASE_URL")
    if not url:
        pytest.skip("EP_TEST_DATABASE_URL must identify a disposable PostgreSQL database")
    from execution_plane.config import get_ep_settings

    settings = get_ep_settings()
    previous = settings.credential_encryption_key
    settings.credential_encryption_key = base64.urlsafe_b64encode(b"d" * 32).decode()
    schema_name = "ep_direct_test_" + uuid4().hex
    engine = create_async_engine(url, execution_options={"schema_translate_map": {"execution_plane": schema_name}})
    try:
        async with engine.begin() as connection:
            await connection.execute(CreateSchema(schema_name))
            await connection.run_sync(SQLModel.metadata.create_all)
        yield engine, async_sessionmaker(engine, expire_on_commit=False), schema_name
    finally:
        settings.credential_encryption_key = previous
        async with engine.begin() as connection:
            await connection.execute(DropSchema(schema_name, cascade=True, if_exists=True))
        await engine.dispose()


def _trust() -> tuple[dict[str, PeerConfig], dict[str, str]]:
    """Create peer keys and project grants for the in-process mTLS transport."""
    peers: dict[str, PeerConfig] = {}
    private_keys = {}
    grant = PeerGrant(
        project_ids=frozenset({UUID(int=81)}),
        permissions={
            "work.execute",
            "work.execute.completed",
            "commands:read",
            "capabilities:read",
        },
    )
    for peer_id, role in (("ao", "controller"), ("ep", "executor")):
        key = ec.generate_private_key(ec.SECP256R1())
        private_keys[peer_id] = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode()
        public_key = (
            key.public_key()
            .public_bytes(
                serialization.Encoding.PEM,
                serialization.PublicFormat.SubjectPublicKeyInfo,
            )
            .decode()
        )
        peers[peer_id] = PeerConfig(
            peer_id=peer_id,
            issuer=f"urn:{peer_id}",
            audience=f"urn:{peer_id}",
            role=role,
            command_url=f"https://{peer_id}.test/v1/commands",
            query_base_url=f"https://{peer_id}.test/v1",
            grants={"ao": grant},
            verification_keys={"test": public_key},
            permitted_networks=["192.0.2.0/24"],
        )
    return peers, private_keys


async def test_ep_direct_runtime_executes_work_and_delivers_completion_without_journal():  # noqa: PLR0915
    async with ep_domain_database() as (engine, sessions, schema_name):
        peers, private_keys = _trust()
        apps: dict[str, FastAPI] = {}
        callback_event = asyncio.Event()
        callbacks: list[MessageEnvelope] = []

        class Network(httpx.AsyncBaseTransport):
            def __init__(self, source: str, destination: str) -> None:
                self.source = source
                self.destination = destination

            async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
                request.headers["x-test-peer"] = self.source
                asgi = httpx.ASGITransport(app=apps[self.destination], raise_app_exceptions=False)
                return await asgi.handle_async_request(request)

        services = {}
        for local_id, local_role in (("ao", "controller"), ("ep", "executor")):
            catalog, registry = CommandCatalog(), CommandRegistry()
            if local_id == "ep":
                registry.register(catalog.definition("work.execute", 1), WorkCommandHandler())
                queries = EPDirectCommandQueryProvider(sessions)
            else:
                queries = None

                class CompletionHandler:
                    async def handle(self, message, _context, session) -> HandlingResult:
                        assert session is None
                        callbacks.append(message)
                        callback_event.set()
                        return HandlingResult(status=CommandStatus.SUCCEEDED)

                registry.register(catalog.definition("work.execute.completed", 1), CompletionHandler())
            signer = AssertionSigner(
                local_id=local_id,
                issuer=peers[local_id].issuer,
                key_id="test",
                private_key=private_keys[local_id],
                peers=peers,
            )
            verifier = AssertionVerifier(
                local_id=local_id,
                audience=peers[local_id].audience,
                command_url=peers[local_id].command_url,
                peers=peers,
            )
            transport = HttpCommandTransport(
                signer=signer,
                peers=peers,
                transports={remote: Network(local_id, remote) for remote in peers if remote != local_id},
            )

            async def certificate_auth(request: Request) -> str:
                return request.headers["x-test-peer"]

            service = build_direct_service(
                local_id=local_id,
                local_role=local_role,
                command_url=peers[local_id].command_url,
                peers=peers,
                registry=registry,
                transport=transport,
                verifier=verifier,
                certificate_auth=certificate_auth,
                queries=queries,
                sessions=sessions if local_id == "ep" else None,
            )
            app = FastAPI()
            app.include_router(service.create_router())
            apps[local_id] = app
            await service.start()
            services[local_id] = service

        try:
            scope = CommandScope(client_id="ao", project_id=UUID(int=81))
            payload = WorkExecute(
                work_correlation_id=UUID(int=82),
                image="python:3.12",
                invocation=NodeInvocation(
                    inputs={},
                    credentials={},
                    workflow_context={},
                    settings={},
                    timeout_seconds=30,
                    max_output_bytes=4096,
                ),
            )
            ao = services["ao"]
            ep = services["ep"]
            receipt = await ao.commands.work_execute(command_id=UUID(int=83), scope=scope, payload=payload)
            assert receipt.state == "delivered"

            async with asyncio.timeout(5):
                while True:
                    status = await ao.client.get(scope, UUID(int=83), peer_id="ep")
                    if status.resource is not None:
                        break
                    await asyncio.sleep(0.02)

            item_id = status.resource.resource_id
            worker_store = WorkStore.from_engine(engine)
            worker_store.set_completion_recorder(DirectCompletionRecorder(transport=ep.transport, queries=ep.queries))
            await worker_store.set_result(item_id, {"script_result": 42}, WorkItemStatus.COMPLETED)
            await worker_store.close()

            async with asyncio.timeout(5):
                await callback_event.wait()
            completion = callbacks[0]
            assert completion.command.command_type == "work.execute.completed"
            assert completion.command.target_url == peers["ao"].command_url
            final = await ao.client.get(scope, UUID(int=83), peer_id="ep")
            assert final.status is CommandStatus.SUCCEEDED
            async with sessions() as session:
                table_names = await session.execute(
                    text("SELECT table_name FROM information_schema.tables WHERE table_schema = :schema"),
                    {"schema": schema_name},
                )
                assert "exchange_journal" not in set(table_names.scalars())
        finally:
            await asyncio.gather(*(service.close() for service in services.values()))
