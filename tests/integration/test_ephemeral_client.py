"""Exercise bilateral direct HTTP commands without Journal storage or sessions."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi import FastAPI, Request

from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.commands import CommandIdentity, CommandRoute
from execution_plane_client.commands.work import NodeInvocation, WorkExecute, work_execute
from execution_plane_client.crypto import canonical_bytes
from execution_plane_client.handlers import CommandRegistry, HandlingResult
from execution_plane_client.protocol import CommandScope, CommandStatus, MessageEnvelope
from execution_plane_client.security import AssertionSigner, AssertionVerifier, PeerConfig, PeerGrant
from execution_plane_transport_direct.service import build_direct_service
from execution_plane_transport_direct.transport import HttpCommandTransport


@asynccontextmanager
async def direct_pair():
    apps: dict[str, FastAPI] = {}
    network_sends: list[tuple[str, str, str]] = []
    private_keys: dict[str, str] = {}
    peers: dict[str, PeerConfig] = {}
    project_id = UUID(int=3)
    permissions = frozenset(
        {
            "work.execute",
            "work.execute.completed",
            "commands:read",
            "capabilities:read",
        }
    )
    grant = PeerGrant(project_ids=frozenset({project_id}), permissions=permissions)

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

    class Network(httpx.AsyncBaseTransport):
        def __init__(self, source: str, destination: str) -> None:
            self.source = source
            self.destination = destination

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            request.headers["x-test-peer"] = self.source
            if request.method == "POST":
                message = MessageEnvelope.model_validate_json(request.content)
                network_sends.append((self.source, self.destination, message.command.command_type))
            asgi_transport = httpx.ASGITransport(app=apps[self.destination], raise_app_exceptions=False)
            response = await asgi_transport.handle_async_request(request)
            if request.method == "POST" and response.status_code == 202:
                await response.aread()
                await response.aclose()
                return httpx.Response(202, content=b"ignored receipt body", request=request)
            return response

    services = {}
    callback_received = asyncio.Event()
    callback_messages: list[MessageEnvelope] = []
    work_calls: list[UUID] = []

    class WorkHandler:
        async def handle(self, message, _context, session) -> HandlingResult:
            assert session is None
            work_calls.append(message.command_id)
            return HandlingResult(
                status=CommandStatus.SUCCEEDED,
                result={"script_result": 42},
                completion_required=True,
            )

    class CompletionHandler:
        async def handle(self, message, _context, session) -> HandlingResult:
            assert session is None
            callback_messages.append(message)
            callback_received.set()
            return HandlingResult(status=CommandStatus.SUCCEEDED)

    for local_id, local_role in (("ao", "controller"), ("ep", "executor")):
        catalog = CommandCatalog()
        registry = CommandRegistry()
        registry.register(catalog.definition("work.execute", 1), WorkHandler())
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
        )
        app = FastAPI()
        app.include_router(service.create_router())
        apps[local_id] = app
        await service.start()
        services[local_id] = SimpleNamespace(app=app, service=service, signer=signer)

    pair = SimpleNamespace(
        peers=peers,
        services=services,
        project_id=project_id,
        network_sends=network_sends,
        callback_received=callback_received,
        callback_messages=callback_messages,
        work_calls=work_calls,
    )
    try:
        yield pair
    finally:
        await asyncio.gather(*(item.service.close() for item in services.values()))


def work_payload() -> WorkExecute:
    return WorkExecute(
        work_correlation_id=uuid4(),
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


async def test_direct_script_submission_and_completion_callback_without_journal():
    async with direct_pair() as pair:
        ao = pair.services["ao"].service
        scope = CommandScope(client_id="ao", project_id=pair.project_id)
        command_id = UUID(int=900)
        receipt = await ao.commands.work_execute(command_id=command_id, scope=scope, payload=work_payload())

        assert receipt.state == "delivered"
        # The direct receiver does not acknowledge until its handler has run.
        assert pair.work_calls == [command_id]
        assert pair.callback_received.is_set()
        async with asyncio.timeout(3):
            while True:
                status = await ao.client.get(scope, command_id, peer_id="ep")
                if status.status == CommandStatus.SUCCEEDED and pair.callback_received.is_set():
                    break
                await asyncio.sleep(0.01)

        assert pair.work_calls == [command_id]
        assert [item.command.command_type for item in pair.callback_messages] == ["work.execute.completed"]
        assert pair.callback_messages[0].command.target_url == pair.peers["ao"].command_url
        assert [sent[2] for sent in pair.network_sends] == ["work.execute", "work.execute.completed"]
        assert status.latest_child_sequence == 1


async def test_direct_retry_of_same_envelope_does_not_repeat_handler_effect():
    async with direct_pair() as pair:
        ao = pair.services["ao"]
        ep = pair.services["ep"]
        scope = CommandScope(client_id="ao", project_id=pair.project_id)
        route = ao.service.client.route(scope, peer_id="ep")
        command_id = UUID(int=903)
        command = work_execute(
            identity=CommandIdentity(command_id=command_id, message_id=UUID(int=904), occurred_at=datetime.now(UTC)),
            route=CommandRoute(
                source_id=route.source_id,
                destination_id=route.destination_id,
                scope=scope,
                target_url=route.target_url,
                callback_url=route.callback_url,
            ),
            payload=work_payload(),
        )
        headers = {
            "authorization": f"Bearer {ao.signer.sign(command)}",
            "x-test-peer": "ao",
        }
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=ep.app, raise_app_exceptions=False),
            base_url="https://ep.test",
        ) as client:
            first = await client.post("/v1/commands", content=canonical_bytes(command), headers=headers)
            duplicate = await client.post("/v1/commands", content=canonical_bytes(command), headers=headers)

        assert first.status_code == duplicate.status_code == 202
        assert first.json()["duplicate"] is False
        assert duplicate.json()["duplicate"] is True
        async with asyncio.timeout(3):
            await pair.callback_received.wait()
        assert pair.work_calls == [command_id]
        assert len(pair.callback_messages) == 1


async def test_direct_ingress_rejects_invalid_identity_signature_revision_and_query_scope():
    async with direct_pair() as pair:
        ao = pair.services["ao"]
        ep = pair.services["ep"]
        scope = CommandScope(client_id="ao", project_id=pair.project_id)
        route = ao.service.client.route(scope, peer_id="ep")
        command_id = UUID(int=901)
        command = work_execute(
            identity=CommandIdentity(command_id=command_id, message_id=UUID(int=902), occurred_at=datetime.now(UTC)),
            route=CommandRoute(
                source_id=route.source_id,
                destination_id=route.destination_id,
                scope=scope,
                target_url=route.target_url,
                callback_url=route.callback_url,
            ),
            payload=work_payload(),
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=ep.app, raise_app_exceptions=False),
            base_url="https://ep.test",
        ) as client:
            token = ao.signer.sign(command)
            bad_signature = await client.post(
                "/v1/commands",
                content=canonical_bytes(command),
                headers={"authorization": "Bearer invalid", "x-test-peer": "ao"},
            )
            bad_certificate = await client.post(
                "/v1/commands",
                content=canonical_bytes(command),
                headers={"authorization": f"Bearer {token}", "x-test-peer": "someone-else"},
            )
            unsupported = command.model_copy(
                update={
                    "command": command.command.model_copy(update={"command_revision": 99}),
                }
            )
            unsupported_response = await client.post(
                "/v1/commands",
                content=canonical_bytes(unsupported),
                headers={"authorization": f"Bearer {ao.signer.sign(unsupported)}", "x-test-peer": "ao"},
            )
            out_of_scope = CommandScope(client_id="ao", project_id=UUID(int=999))
            query_path = f"/v1/commands/{command_id}"
            query_token = ao.signer.sign_query("ep", out_of_scope, query_path)
            bad_query = await client.get(
                query_path,
                headers={"authorization": f"Bearer {query_token}", "x-test-peer": "ao"},
            )

        assert bad_signature.status_code == 403
        assert bad_certificate.status_code == 403
        assert unsupported_response.status_code == 422
        assert bad_query.status_code == 403
        assert pair.work_calls == []
