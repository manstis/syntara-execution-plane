"""Shared API rejects private invalid input without echoing it into responses."""

from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, Request


@pytest.fixture
def api_app(security_case):
    from execution_plane_client.api import create_router
    from execution_plane_client.catalog import CapabilityRegistry
    from execution_plane_client.handlers import CommandRegistry

    async def certificate(request: Request) -> str:
        return request.headers.get("x-test-peer", "ao")

    service = SimpleNamespace(
        certificate_auth=certificate,
        verifier=security_case.verifier,
        capabilities=CapabilityRegistry(CommandRegistry()),
        receive_command=None,
        get_command=None,
        get_children=None,
    )
    app = FastAPI()
    app.include_router(create_router(service))
    return app


async def test_invalid_payload_response_does_not_echo_private_input(api_app):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api_app), base_url="https://ep.test") as client:
        response = await client.post("/v1/commands", json={"credentials": "private-test-secret"})
    assert response.status_code == 422
    assert "private-test-secret" not in response.text
    assert "credentials" not in response.text


async def test_signed_query_also_requires_matching_certificate_identity(api_app, security_case, message):
    token = security_case.signer.sign_query("ep", message.scope, "/v1/capabilities")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api_app), base_url="https://ep.test") as client:
        denied = await client.get(
            "/v1/capabilities", headers={"authorization": f"Bearer {token}", "x-test-peer": "other"}
        )
        allowed = await client.get(
            "/v1/capabilities", headers={"authorization": f"Bearer {token}", "x-test-peer": "ao"}
        )
    assert denied.status_code == 403
    assert allowed.status_code == 200
    assert allowed.json()["commands"] == {}


async def test_same_router_receives_action_and_completion_without_database(message):
    from uuid import UUID

    from execution_plane_client.catalog import CapabilityRegistry
    from execution_plane_client.handlers import CommandRegistry
    from execution_plane_client.protocol import Command, MessageEnvelope
    from execution_plane_client.security import AuthenticatedIdentity
    from execution_plane_client.transport import TransportReceipt

    received = []

    class Verifier:
        def verify(self, _token: str, envelope: MessageEnvelope) -> AuthenticatedIdentity:
            return AuthenticatedIdentity(
                peer_id=envelope.source_id,
                scope=envelope.scope,
                scopes=frozenset({envelope.command.command_type}),
                role="controller",
            )

        def verify_query(self, _token: str, _path: str) -> AuthenticatedIdentity:
            return AuthenticatedIdentity(
                peer_id="ao",
                scope=message.scope,
                scopes=frozenset({"capabilities:read"}),
                role="controller",
            )

    async def certificate(request: Request) -> str:
        return request.headers["x-test-peer"]

    async def receive(envelope: MessageEnvelope, _identity: AuthenticatedIdentity) -> TransportReceipt:
        received.append(envelope)
        return TransportReceipt(received=True, message_id=envelope.message_id)

    service = SimpleNamespace(
        certificate_auth=certificate,
        verifier=Verifier(),
        capabilities=CapabilityRegistry(CommandRegistry()),
        receive_command=receive,
        get_command=None,
        get_children=None,
    )
    app = FastAPI()
    from execution_plane_client.api import create_router

    app.include_router(create_router(service))
    completion_id = UUID(int=501)
    completion_message = MessageEnvelope(
        message_id=UUID(int=502),
        command_id=completion_id,
        source_id="ep",
        destination_id="ao",
        scope=message.scope,
        command=Command(
            command_id=completion_id,
            parent_command_id=message.command_id,
            parent_sequence=1,
            command_type="work.execute.completed",
            command_revision=1,
            target_url=message.command.callback_url,
            callback_url=message.command.target_url,
            occurred_at=message.command.occurred_at,
            payload={"status": "succeeded", "result": {}},
        ),
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ao.test") as client:
        action_response = await client.post(
            "/v1/commands",
            content=message.model_dump_json(),
            headers={"authorization": "Bearer test", "x-test-peer": "ao"},
        )
        completion_response = await client.post(
            "/v1/commands",
            content=completion_message.model_dump_json(),
            headers={"authorization": "Bearer test", "x-test-peer": "ep"},
        )

    assert action_response.status_code == completion_response.status_code == 202
    assert [envelope.command.command_type for envelope in received] == [
        "work.execute",
        "work.execute.completed",
    ]
