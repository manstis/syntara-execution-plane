"""Bounded in-process command projections used by direct runtimes."""

from uuid import uuid4

import pytest

from execution_plane_client.errors import CommandNotFound, UnauthorizedPeer
from execution_plane_transport_direct.queries import InMemoryCommandQueryProvider


async def test_in_memory_queries_deduplicate_scope_and_bound_retention(message):
    provider = InMemoryCommandQueryProvider(max_commands=2)

    assert provider.record_received(message, local_id="ep")
    assert not provider.record_received(message, local_id="ep")
    assert (await provider.get(message.scope, message.command_id, peer_id="ao")).status == "accepted"

    with pytest.raises(UnauthorizedPeer):
        await provider.get(message.scope, message.command_id, peer_id="other")

    second_command_id = uuid4()
    second = message.model_copy(
        update={
            "message_id": uuid4(),
            "command_id": second_command_id,
            "command": message.command.model_copy(update={"command_id": second_command_id}),
        }
    )
    provider.record_received(second, local_id="ep")

    third_command_id = uuid4()
    third = message.model_copy(
        update={
            "message_id": uuid4(),
            "command_id": third_command_id,
            "command": message.command.model_copy(update={"command_id": third_command_id}),
        }
    )
    provider.record_received(third, local_id="ep")

    assert len(provider._entries) == 2
    assert len(provider._seen_messages) == 2
    with pytest.raises(CommandNotFound):
        await provider.get(message.scope, message.command_id, peer_id="ao")
