"""EP's direct runtime must execute provider work without Journal storage."""

from __future__ import annotations

import subprocess
import sys
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Self
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from execution_plane_client.commands import CommandIdentity, CommandRoute
from execution_plane_client.commands.work import NodeInvocation, WorkExecute, work_execute
from execution_plane_client.errors import UnauthorizedPeer
from execution_plane_client.protocol import CommandScope, CommandStatus, MessageEnvelope
from execution_plane_client.transport import DispatchReceipt, DispatchState
from execution_plane_transport_direct.queries import InMemoryCommandQueryProvider


def _work_message(security_case: SimpleNamespace) -> MessageEnvelope:
    scope = CommandScope(client_id="ao", project_id=UUID(int=3))
    route = CommandRoute(
        source_id="ao",
        destination_id="ep",
        scope=scope,
        target_url="https://ep.test/v1/commands",
        callback_url="https://ao.test/v1/commands",
    )
    payload = WorkExecute(
        work_correlation_id=UUID(int=4),
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
    return work_execute(
        identity=CommandIdentity(command_id=UUID(int=5), message_id=UUID(int=6), occurred_at=security_case.now),
        route=route,
        payload=payload,
    )


def _case() -> SimpleNamespace:
    return SimpleNamespace(now=datetime(2026, 10, 13, tzinfo=UTC))


async def test_work_handler_can_enqueue_without_a_journal(monkeypatch):
    from execution_plane.commands.work import WorkCommandHandler
    from execution_plane.work_store import WorkStore

    item = SimpleNamespace(id=UUID(int=7))
    dispatch = AsyncMock(return_value=item)
    monkeypatch.setattr(WorkStore, "dispatch", dispatch)
    message = _work_message(_case())
    handler = WorkCommandHandler()

    result = await handler.handle(message, SimpleNamespace(), SimpleNamespace())

    assert result.status is CommandStatus.RUNNING
    payload = dispatch.await_args.kwargs["payload"]
    assert payload["_ep_command_id"] == str(message.command_id)
    assert payload["_ep_completion_context"] == {
        "message_id": str(message.message_id),
        "command_id": str(message.command_id),
        "source_id": message.source_id,
        "destination_id": message.destination_id,
        "scope": message.scope.model_dump(mode="json"),
        "command_type": message.command.command_type,
        "command_revision": message.command.command_revision,
        "target_url": message.command.target_url,
        "callback_url": message.command.callback_url,
        "occurred_at": message.command.occurred_at.isoformat().replace("+00:00", "Z"),
        "work_correlation_id": str(UUID(int=4)),
    }


async def test_direct_completion_is_published_after_terminal_state():
    from execution_plane.models.work_item import WorkItem, WorkItemStatus
    from execution_plane_transport_direct.completion_recorder import DirectCompletionRecorder

    case = _case()
    original = _work_message(case)
    queries = InMemoryCommandQueryProvider()
    queries.record_received(original, local_id="ep")
    queries.mark_status(original.scope, original.command_id, CommandStatus.RUNNING)
    transport = AsyncMock()
    transport.send.return_value = DispatchReceipt(message_id=UUID(int=11), state=DispatchState.DELIVERED)
    payload = {
        "_ep_completion_context": {
            "message_id": str(original.message_id),
            "command_id": str(original.command_id),
            "source_id": original.source_id,
            "destination_id": original.destination_id,
            "scope": original.scope.model_dump(mode="json"),
            "command_type": original.command.command_type,
            "command_revision": original.command.command_revision,
            "target_url": original.command.target_url,
            "callback_url": original.command.callback_url,
            "occurred_at": original.command.occurred_at.isoformat().replace("+00:00", "Z"),
            "work_correlation_id": str(UUID(int=4)),
        }
    }
    item = WorkItem(
        id=UUID(int=7),
        client_id="ao",
        project_id=UUID(int=3),
        request_id=str(original.command_id),
        request_hash="f" * 64,
        work_correlation_id=UUID(int=4),
        status=WorkItemStatus.COMPLETED,
        payload=payload,
        result={"script_result": 42},
        created_at=datetime.now(UTC),
        completed_at=case.now,
    )
    recorder = DirectCompletionRecorder(transport=transport, queries=queries)

    await recorder.after_commit(item)

    transport.send.assert_awaited_once()
    child = transport.send.await_args.args[0]
    assert child.command.command_type == "work.execute.completed"
    assert child.command.target_url == original.command.callback_url
    assert child.command.payload["status"] == "succeeded"
    status = await queries.get(original.scope, original.command_id, peer_id="ao")
    assert status.status is CommandStatus.SUCCEEDED
    children = await queries.children(original.scope, original.command_id, peer_id="ao")
    assert children.items[0].command_id == child.command_id


async def test_ep_command_queries_poll_provider_state_after_runtime_process_loss():
    from execution_plane.models.work_item import WorkItem, WorkItemStatus
    from execution_plane_transport_direct.ep_queries import EPDirectCommandQueryProvider

    original = _work_message(_case())
    payload = {
        "_ep_completion_context": {
            "message_id": str(original.message_id),
            "command_id": str(original.command_id),
            "source_id": original.source_id,
            "destination_id": original.destination_id,
            "scope": original.scope.model_dump(mode="json"),
            "command_type": original.command.command_type,
            "command_revision": original.command.command_revision,
            "target_url": original.command.target_url,
            "callback_url": original.command.callback_url,
            "occurred_at": original.command.occurred_at.isoformat().replace("+00:00", "Z"),
            "work_correlation_id": str(UUID(int=4)),
        }
    }
    item = WorkItem(
        id=UUID(int=7),
        client_id="ao",
        project_id=UUID(int=3),
        request_id=str(original.command_id),
        request_hash="f" * 64,
        work_correlation_id=UUID(int=4),
        status=WorkItemStatus.COMPLETED,
        payload=payload,
        result={"script_result": 42},
        created_at=_case().now,
        completed_at=_case().now,
    )

    class Session:
        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def scalar(self, _statement: object) -> WorkItem:
            return item

    provider = EPDirectCommandQueryProvider(Session)

    read = await provider.get(original.scope, original.command_id, peer_id="ao")
    children = await provider.children(original.scope, original.command_id, peer_id="ao", include_recovery=True)

    assert read.status is CommandStatus.SUCCEEDED
    assert children.items[0].status is CommandStatus.SUCCEEDED
    assert children.items[0].recovery_message.command.target_url == original.command.callback_url
    assert (await provider.get(original.scope, original.command_id, peer_id="ep")).status is CommandStatus.SUCCEEDED
    destination_children = await provider.children(original.scope, original.command_id, peer_id="ep")
    assert len(destination_children.items) == 1
    assert destination_children.items[0].recovery_message is None

    with pytest.raises(UnauthorizedPeer):
        await provider.children(original.scope, original.command_id, peer_id="ep", include_recovery=True)

    with pytest.raises(UnauthorizedPeer):
        await provider.get(original.scope, original.command_id, peer_id="intruder")
    with pytest.raises(UnauthorizedPeer):
        await provider.children(original.scope, original.command_id, peer_id="intruder")


def test_execution_plane_domain_and_api_modules_do_not_import_transports():
    script = """
import sys
import execution_plane.api.main
import execution_plane.commands.clusters
import execution_plane.commands.control
import execution_plane.commands.queries
import execution_plane.commands.resources
import execution_plane.commands.targets
import execution_plane.commands.work
import execution_plane.commands.service

assert not any(name.startswith('execution_plane_transport_direct') for name in sys.modules)
assert not any(name.startswith('execution_plane_transport_journal') for name in sys.modules)
"""
    subprocess.run([sys.executable, "-c", script], check=True)
