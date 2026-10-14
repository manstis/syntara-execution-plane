"""The replacement API accepts commands and exposes only domain resource state."""

from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest

from execution_plane.api.auth import ServiceIdentity, get_service_identity
from execution_plane.api.main import create_app


def command_message(kind="command.cancel", payload=None):
    command_id = uuid4()
    return {
        "message_id": str(uuid4()),
        "command_id": str(command_id),
        "source_id": "ao",
        "destination_id": "execution-plane",
        "scope": {"client_id": "ao"},
        "command": {
            "command_id": str(command_id),
            "command_type": kind,
            "command_revision": 1,
            "occurred_at": datetime.now(UTC).isoformat(),
            "payload": payload if payload is not None else {"target_command_id": str(uuid4())},
        },
    }


def test_only_command_mutations_and_resource_queries_are_exposed():
    paths = create_app().openapi()["paths"]
    assert "/v1/commands" in paths
    assert "/v1/commands/{command_id}" not in paths
    assert "/v1/commands/{command_id}/children" not in paths
    assert not any("cluster-bindings" in path for path in paths)
    assert [
        (path, method)
        for path, methods in paths.items()
        for method in methods
        if method in {"post", "put", "patch", "delete"}
    ] == [("/v1/commands", "post")]
    for resource in ("work-items", "clusters", "execution-targets"):
        assert f"/v1/{resource}" in paths
        assert f"/v1/{resource}/{{resource_id}}" in paths


@pytest.mark.parametrize(
    ("change", "status"), [("owner", 403), ("revision", 422), ("private", 422), ("permission", 403)]
)
async def test_ingress_rejects_before_domain_effects(change, status):
    app = create_app()
    scopes = frozenset() if change == "permission" else frozenset({"command.cancel"})
    app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(client_id="ao", scopes=scopes)
    message = command_message()
    if change == "owner":
        message["scope"]["client_id"] = "other"
    elif change == "revision":
        message["command"]["command_revision"] = 99
    elif change == "private":
        message["command"]["payload"] = {"credential": "private-secret"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        response = await client.post("/v1/commands", json=message)
    assert response.status_code == status
    assert "private-secret" not in response.text


async def test_strict_json_rejects_duplicate_keys():
    app = create_app()
    app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(client_id="ao", scopes=frozenset())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        response = await client.post("/v1/commands", content=b'{"private":"secret","private":"other"}')
    assert response.status_code == 422
    assert "secret" not in response.text


async def test_ingress_rejects_excessive_json_nesting():
    app = create_app()
    app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(client_id="ao", scopes=frozenset())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        response = await client.post("/v1/commands", content=b"[" * 100000 + b"0" + b"]" * 100000)
    assert response.status_code == 422


async def test_ingress_bounds_body_and_requires_authentication():
    app = create_app()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        unauthenticated = await client.post("/v1/commands", json=command_message())
        assert unauthenticated.status_code == 401
        app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(client_id="ao", scopes=frozenset())
        oversized = await client.post("/v1/commands", content=b" " * (1024 * 1024 + 1))
        assert oversized.status_code == 413


async def test_capabilities_do_not_advertise_uninstalled_completion_handlers():
    app = create_app()
    app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(
        client_id="ao", scopes=frozenset({"capabilities:read"})
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        response = await client.get("/v1/capabilities")
    assert response.status_code == 200
    commands = response.json()["commands"]
    assert len(commands) == 9
    assert commands["command.cancel"] == [1]
    assert "work.execute.completed" not in commands
