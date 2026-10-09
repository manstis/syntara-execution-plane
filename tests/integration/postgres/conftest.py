"""Shared PostgreSQL protocol fixtures."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from execution_plane_client.commands import CommandIdentity, CommandRoute
from execution_plane_client.commands.work import WorkExecute, work_execute


@pytest.fixture
def work_message():
    return work_execute(
        identity=CommandIdentity(command_id=uuid4(), message_id=uuid4(), occurred_at=datetime(2026, 10, 9, tzinfo=UTC)),
        route=CommandRoute(
            source_id="ao",
            destination_id="ep",
            scope={"client_id": "ao", "project_id": UUID(int=3), "all_projects": False},
            target_url="https://ep.test/v1/commands",
            callback_url="https://ao.test/v1/commands",
        ),
        payload=WorkExecute(
            work_correlation_id=uuid4(),
            image="node:test",
            invocation={
                "inputs": {},
                "credentials": {},
                "workflow_context": {},
                "settings": {},
                "timeout_seconds": 30,
                "max_output_bytes": 1024,
            },
        ),
    )
