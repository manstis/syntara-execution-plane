"""Client commands dispatch immediately without database sessions."""

from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.handlers import CommandDispatcher, CommandRegistry
from execution_plane_client.protocol import MessageEnvelope
from execution_plane_client.transport import DispatchReceipt, DispatchState


async def test_client_submit_dispatches_without_database_transaction(work_message):
    from execution_plane_client.client import MediatorClient
    from execution_plane_client.security import PeerConfig

    class RecordingTransport(CommandDispatcher):
        def __init__(self) -> None:
            self.messages = []

        async def send(self, message: MessageEnvelope) -> DispatchReceipt:
            self.messages.append(message)
            return DispatchReceipt(message_id=message.message_id, state=DispatchState.DELIVERED)

    catalog = CommandCatalog()
    registry = CommandRegistry()
    registry.register(catalog.definition("work.execute.completed", 1), object())
    transport = RecordingTransport()
    peer = PeerConfig(
        peer_id="ep",
        issuer="urn:ep",
        audience="urn:ep",
        role="executor",
        command_url="https://ep.test/v1/commands",
        query_base_url="https://ep.test/v1",
        grants={},
        verification_keys={},
        permitted_networks=["192.0.2.0/24"],
    )
    client = MediatorClient(
        catalog,
        registry,
        transport,
        local_id="ao",
        local_role="controller",
        command_url="https://ao.test/v1/commands",
        peers={"ep": peer},
    )

    receipt = await client.submit(work_message)

    assert receipt.message_id == work_message.message_id
    assert receipt.state is DispatchState.DELIVERED
    assert transport.messages == [work_message]
