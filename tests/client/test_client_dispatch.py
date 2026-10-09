"""Client submission validates envelopes and dispatches immediately."""

from uuid import UUID

import pytest

from execution_plane_client.handlers import CommandDispatcher


class RecordingTransport(CommandDispatcher):
    """Capture sends and return a configured dispatch receipt."""

    def __init__(self, receipt) -> None:
        """Configure the fixed receipt returned to each send."""
        self.receipt = receipt
        self.sent = []

    async def send(self, message):
        self.sent.append(message)
        return self.receipt


def make_client(security_case, transport):
    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.client import MediatorClient
    from execution_plane_client.handlers import CommandRegistry

    catalog = CommandCatalog()
    registry = CommandRegistry()
    registry.register(catalog.definition("work.execute.completed", 1), object())
    return MediatorClient(
        catalog,
        registry,
        transport,
        local_id="ao",
        local_role="controller",
        command_url="https://ao.test/v1/commands",
        peers=security_case.peers,
    )


def make_valid_work_message():
    from datetime import UTC, datetime

    from execution_plane_client.commands import CommandIdentity, CommandRoute
    from execution_plane_client.commands.work import NodeInvocation, WorkExecute, work_execute
    from execution_plane_client.protocol import CommandScope

    command_id = UUID(int=300)
    return work_execute(
        identity=CommandIdentity(command_id=command_id, message_id=UUID(int=301), occurred_at=datetime.now(UTC)),
        route=CommandRoute(
            source_id="ao",
            destination_id="ep",
            scope=CommandScope(client_id="ao", project_id=UUID(int=3)),
            target_url="https://ep.test/v1/commands",
            callback_url="https://ao.test/v1/commands",
        ),
        payload=WorkExecute(
            work_correlation_id=UUID(int=302),
            image="python:3.12",
            invocation=NodeInvocation(
                inputs={},
                credentials={},
                workflow_context={},
                settings={},
                timeout_seconds=30,
                max_output_bytes=4096,
            ),
        ),
    )


async def test_submit_validates_then_dispatches_once_and_returns_receipt(security_case):
    message = make_valid_work_message()
    from execution_plane_client.transport import DispatchReceipt, DispatchState

    receipt = DispatchReceipt(message_id=message.message_id, state=DispatchState.DELIVERED)
    transport = RecordingTransport(receipt)
    client = make_client(security_case, transport)

    result = await client.submit(message)

    assert result is receipt
    assert transport.sent == [message]


async def test_submit_rejects_invalid_route_without_dispatch(security_case):
    from execution_plane_client.errors import UnsafeDestination

    message = make_valid_work_message()
    transport = RecordingTransport(None)
    client = make_client(security_case, transport)
    invalid = message.model_copy(
        update={"command": message.command.model_copy(update={"target_url": "https://attacker.test/commands"})}
    )

    with pytest.raises(UnsafeDestination):
        await client.submit(invalid)

    assert transport.sent == []


async def test_typed_work_execute_builds_and_dispatches_without_session(security_case):
    from execution_plane_client.commands.work import NodeInvocation, WorkExecute
    from execution_plane_client.dispatch import ExecutionPlaneCommands
    from execution_plane_client.protocol import CommandScope
    from execution_plane_client.transport import DispatchReceipt, DispatchState

    command_id = UUID(int=200)
    scope = CommandScope(client_id="ao", project_id=UUID(int=3))
    payload = WorkExecute(
        work_correlation_id=UUID(int=201),
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
    receipt = DispatchReceipt(message_id=UUID(int=202), state=DispatchState.DELIVERED)
    transport = RecordingTransport(receipt)
    client = make_client(security_case, transport)

    result = await ExecutionPlaneCommands(client).work_execute(
        command_id=command_id,
        scope=scope,
        payload=payload,
    )

    assert result is receipt
    assert len(transport.sent) == 1
    assert transport.sent[0].command.command_type == "work.execute"
    assert transport.sent[0].command.payload["work_correlation_id"] == str(payload.work_correlation_id)
