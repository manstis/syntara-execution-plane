"""Credential-resolution callback client contracts."""

from __future__ import annotations

import json
from uuid import uuid4

import httpx
import pytest

from execution_plane.callback_client import CallbackHTTPClient
from execution_plane.config import EPSettings
from execution_plane.credential_resolution.resolver import (
    CredentialResolutionMismatchError,
    CredentialResolver,
)


@pytest.mark.asyncio
async def test_credential_resolver_posts_command_and_returns_correlated_value():
    credential_ref = uuid4()
    requests: list[httpx.Request] = []

    def receive(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        body = json.loads(request.content)
        assert isinstance(body, dict)
        message_id = body["message_id"]
        return httpx.Response(200, json={"message_id": message_id, "credential": "resolved-token"})

    settings = EPSettings(EP_CREDENTIAL_RESOLUTION_CALLBACK_URL="https://ao.example.test/credentials/resolve")
    callback_client = CallbackHTTPClient(url=settings.credential_resolution_callback_url)
    await callback_client.close()
    callback_client._client = httpx.AsyncClient(transport=httpx.MockTransport(receive))
    resolver = CredentialResolver(settings, client=callback_client)

    try:
        credential = await resolver.resolve(credential_ref, client_id="syntara-orchestration")
    finally:
        await resolver.close()

    assert credential == "resolved-token"
    assert len(requests) == 1
    body = json.loads(requests[0].content)
    assert body["command"]["command_type"] == "credential.resolve"
    assert body["command"]["payload"] == {"credential_ref": str(credential_ref)}
    assert body["source_id"] == "execution-plane"
    assert body["destination_id"] == "syntara-orchestration"
    assert body["scope"]["client_id"] == "syntara-orchestration"


@pytest.mark.asyncio
async def test_credential_resolver_rejects_an_unrelated_response():
    credential_ref = uuid4()
    settings = EPSettings(EP_CREDENTIAL_RESOLUTION_CALLBACK_URL="https://ao.example.test/credentials/resolve")
    callback_client = CallbackHTTPClient(url=settings.credential_resolution_callback_url)
    await callback_client.close()
    callback_client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json={"message_id": str(uuid4()), "credential": "x"})
        )
    )
    resolver = CredentialResolver(settings, client=callback_client)

    try:
        with pytest.raises(CredentialResolutionMismatchError):
            await resolver.resolve(credential_ref, client_id="syntara-orchestration")
    finally:
        await resolver.close()
