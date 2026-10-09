"""Wire validation catches ambiguous identities and nonportable payloads."""

from datetime import UTC
from uuid import UUID

import pytest
from pydantic import ValidationError


def test_valid_wire_round_trip(wire_message):
    from execution_plane_client.protocol import MessageEnvelope

    message = MessageEnvelope.model_validate(wire_message)
    assert message.command_id == UUID(int=2)
    assert message.command.occurred_at.tzinfo is UTC
    assert MessageEnvelope.model_validate_json(message.model_dump_json()) == message


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("command_id", str(UUID(int=99))),
        ("sequence", -1),
        ("sequence", True),
        ("source_id", ""),
        ("unexpected", "secret"),
    ],
)
def test_invalid_envelope(wire_message, field, value):
    from execution_plane_client.protocol import MessageEnvelope

    wire_message[field] = value
    with pytest.raises(ValidationError):
        MessageEnvelope.model_validate(wire_message)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("occurred_at", "2026-10-09T10:00:00"),
        ("command_revision", 0),
        ("command_revision", True),
        ("target_url", "http://ep.test/v1/commands"),
        ("callback_url", "https://user:password@ao.test/v1/commands"),
        ("callback_url", "https://ao.test/v1/commands?token=private"),
        ("unknown", 42),
    ],
)
def test_invalid_command(wire_message, field, value):
    from execution_plane_client.protocol import MessageEnvelope

    wire_message["command"][field] = value
    with pytest.raises(ValidationError):
        MessageEnvelope.model_validate(wire_message)


@pytest.mark.parametrize(
    "payload",
    [
        {"number": float("nan")},
        {"number": float("inf")},
        {"data": b"secret"},
        {"data": (1, 2)},
        {1: "non-string key"},
        {"text": "x" * (512 * 1024)},
        {"nested": {"items": [object()]}},
    ],
)
def test_invalid_private_payload(wire_message, payload):
    from execution_plane_client.protocol import MessageEnvelope

    wire_message["command"]["payload"] = payload
    with pytest.raises(ValidationError):
        MessageEnvelope.model_validate(wire_message)


@pytest.mark.parametrize(
    "scope",
    [
        {"client_id": "", "project_id": None, "all_projects": True},
        {"client_id": "ao", "project_id": None, "all_projects": False},
        {"client_id": "ao", "project_id": str(UUID(int=3)), "all_projects": True},
    ],
)
def test_scope_is_one_project_or_explicit_all_projects(wire_message, scope):
    from execution_plane_client.protocol import MessageEnvelope

    wire_message["scope"] = scope
    with pytest.raises(ValidationError):
        MessageEnvelope.model_validate(wire_message)


def test_read_contract_rejects_private_payload():
    from execution_plane_client.protocol import CommandRead

    with pytest.raises(ValidationError):
        CommandRead(command_id=UUID(int=2), status="accepted", payload={"token": "secret"})


@pytest.mark.timeout(1)
def test_circular_python_payload_is_rejected(wire_message):
    from execution_plane_client.protocol import MessageEnvelope

    payload = {}
    payload["self"] = payload
    wire_message["command"]["payload"] = payload
    with pytest.raises(ValidationError):
        MessageEnvelope.model_validate(wire_message)
