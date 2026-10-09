"""Immediate HTTP transport reports delivery from HTTP status alone."""

from datetime import UTC, datetime
from uuid import UUID

import httpx


async def test_http_202_with_empty_body_is_delivered():
    from execution_plane_client.protocol import Command, CommandScope, ExecutionPolicy, MessageEnvelope
    from execution_plane_client.security import PeerConfig, PeerGrant
    from execution_plane_transport_direct.transport import HttpCommandTransport

    peer = PeerConfig(
        peer_id="ep",
        issuer="urn:ep",
        audience="urn:ep",
        command_url="https://ep.test/v1/commands",
        query_base_url="https://ep.test/v1",
        grants={"ao": PeerGrant(project_ids=frozenset({UUID(int=3)}), permissions={"work.execute"})},
        verification_keys={"key": "unused-by-this-sender-test"},
        permitted_networks=["192.0.2.0/24"],
    )

    class Signer:
        local_id = "ao"

        def sign(self, _message: MessageEnvelope) -> str:
            return "test-assertion"

    command = Command(
        command_id=UUID(int=2),
        command_type="work.execute",
        command_revision=1,
        target_url=peer.command_url,
        callback_url="https://ao.test/v1/commands",
        payload={},
        occurred_at=datetime(2026, 10, 13, tzinfo=UTC),
        execution_policy=ExecutionPolicy(timeout_seconds=30),
    )
    message = MessageEnvelope(
        message_id=UUID(int=1),
        command_id=command.command_id,
        source_id="ao",
        destination_id="ep",
        scope=CommandScope(client_id="ao", project_id=UUID(int=3)),
        command=command,
    )
    transport = HttpCommandTransport(
        signer=Signer(),
        peers={"ep": peer},
        transports={"ep": httpx.MockTransport(lambda _request: httpx.Response(202, content=b""))},
    )

    try:
        receipt = await transport.send(message)
    finally:
        await transport.aclose()

    assert receipt.message_id == message.message_id
    assert receipt.state == "delivered"
