"""Shared API rejects private invalid input without echoing it into responses."""

from collections.abc import AsyncIterator
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

    async def no_database() -> AsyncIterator[None]:
        yield None

    service = SimpleNamespace(
        sessions=None,
        session_dependency=no_database,
        certificate_auth=certificate,
        verifier=security_case.verifier,
        capabilities=CapabilityRegistry(CommandRegistry()),
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
    assert allowed.json()["commands"] == {"command.accepted": [1]}
