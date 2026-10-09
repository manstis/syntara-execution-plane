"""Independent client wire fixtures."""

from uuid import UUID

import pytest


@pytest.fixture
def wire_message():
    """Return a complete JSON-compatible envelope, without application settings."""
    return {
        "message_id": str(UUID(int=1)),
        "message_type": "command.submit",
        "command_id": str(UUID(int=2)),
        "sequence": 0,
        "source_id": "ao",
        "destination_id": "ep",
        "scope": {"client_id": "ao", "project_id": str(UUID(int=3)), "all_projects": False},
        "command": {
            "command_id": str(UUID(int=2)),
            "parent_command_id": None,
            "command_type": "work.execute",
            "command_revision": 1,
            "target_url": "https://ep.test/v1/commands",
            "callback_url": "https://ao.test/v1/commands",
            "payload": {"invocation": {"script": "print(42)"}, "limit": 1},
            "occurred_at": "2026-10-09T10:00:00Z",
            "parent_sequence": None,
            "execution_policy": {"timeout_seconds": 30, "max_attempts": 1},
        },
    }
