"""Tests for durable completion callback delivery configuration."""

from __future__ import annotations

import ssl
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from execution_plane.completion_notifier import notifier as event_delivery
from execution_plane.config import EPSettings

if TYPE_CHECKING:
    import pytest


def test_callback_client_loads_client_identity_into_ca_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """The callback TLS context must include both the trusted CA and EP client identity."""
    context = MagicMock(spec=ssl.SSLContext)
    context_factory = MagicMock(return_value=context)
    client_factory = MagicMock()
    monkeypatch.setattr(event_delivery.ssl, "create_default_context", context_factory)
    monkeypatch.setattr(event_delivery.httpx, "AsyncClient", client_factory)
    settings = EPSettings(
        EP_DATABASE_URL="postgresql+asyncpg://user:password@localhost/test",
        EP_COMPLETION_CALLBACK_URL="https://ao.example.test/events",
        EP_CALLBACK_CA_CERT_PATH="/secrets/ca.pem",
        EP_CALLBACK_CERT_PATH="/secrets/ep.crt",
        EP_CALLBACK_KEY_PATH="/secrets/ep.key",
    )

    event_delivery.CompletionNotifier(settings)

    context_factory.assert_called_once_with(cafile="/secrets/ca.pem")
    context.load_cert_chain.assert_called_once_with("/secrets/ep.crt", "/secrets/ep.key")
    assert client_factory.call_args.kwargs["verify"] is context
    assert "cert" not in client_factory.call_args.kwargs


async def test_completion_is_a_stable_command_rebuilt_from_domain_event():
    """Retries reuse the event identity and correlate directly with the work UUID."""
    from datetime import UTC, datetime
    from unittest.mock import AsyncMock
    from uuid import uuid4

    import httpx

    from execution_plane.models.completion_event import CompletionEvent
    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.protocol import MessageEnvelope

    bodies = []

    def receive(request) -> httpx.Response:
        envelope = MessageEnvelope.model_validate_json(request.content)
        CommandCatalog().validate(envelope)
        bodies.append(envelope)
        return httpx.Response(202, json={"received": True, "message_id": str(envelope.message_id)})

    settings = EPSettings(EP_COMPLETION_CALLBACK_URL="https://ao.test/v1/commands")
    delivery = event_delivery.CompletionNotifier(settings)
    await delivery.close()
    delivery._client = httpx.AsyncClient(transport=httpx.MockTransport(receive))
    event = CompletionEvent(
        id=uuid4(),
        work_item_id=uuid4(),
        client_id="ao",
        status="completed",
        result={"stdout": "done"},
        created_at=datetime.now(UTC),
        next_attempt_at=datetime.now(UTC),
    )
    store = AsyncMock()
    try:
        await delivery._deliver_one(store, event)
        await delivery._deliver_one(store, event)
    finally:
        await delivery.close()
    assert len(bodies) == 2
    assert bodies[0] == bodies[1]
    assert bodies[0].command.command_type == "work.execute.completed"
    assert bodies[0].command.parent_command_id == event.work_item_id
    assert bodies[0].command.command_id == event.id
    assert bodies[0].command.payload["status"] == "succeeded"
    assert bodies[0].command.payload["result"] == {"stdout": "done"}


async def test_wrong_completion_receipt_is_retried():
    from datetime import UTC, datetime
    from unittest.mock import AsyncMock
    from uuid import uuid4

    import httpx

    from execution_plane.models.completion_event import CompletionEvent

    delivery = event_delivery.CompletionNotifier(EPSettings(EP_COMPLETION_CALLBACK_URL="https://ao.test/v1/commands"))
    await delivery.close()
    delivery._client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(202, json={"received": True, "message_id": str(uuid4())})
        )
    )
    event = CompletionEvent(
        id=uuid4(),
        work_item_id=uuid4(),
        client_id="ao",
        status="failed",
        result={},
        created_at=datetime.now(UTC),
        next_attempt_at=datetime.now(UTC),
    )
    store = AsyncMock()
    try:
        await delivery._deliver_one(store, event)
    finally:
        await delivery.close()
    assert store.record_completion_event_attempt.call_args.kwargs["delivered"] is False
