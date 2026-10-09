"""EP delivery uses the shared protocol runtime and configures the work recorder."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from execution_plane.commands.service import EPCommandService


async def test_shared_runtime_is_closed_and_recorder_is_installed():
    runtime = SimpleNamespace(
        journal=object(), sessions=MagicMock(), close=AsyncMock(), work_completion_recorder=object()
    )
    store = MagicMock()
    delivery = EPCommandService(runtime)
    delivery.configure_store(store)
    store.set_completion_recorder.assert_called_once()
    await delivery.close()
    runtime.close.assert_awaited_once()


async def test_close_stops_and_awaits_shared_runtime():
    import asyncio

    started = asyncio.Event()
    stopped = []

    async def poll() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.append(True)

    runtime = SimpleNamespace(
        journal=object(), sessions=MagicMock(), close=AsyncMock(), run=poll, work_completion_recorder=object()
    )
    delivery = EPCommandService(runtime)
    task = asyncio.create_task(delivery.run(MagicMock()))
    await started.wait()
    await delivery.close()
    await asyncio.wait_for(task, timeout=1)
    assert len(stopped) == 1
    runtime.close.assert_awaited_once()


async def test_ephemeral_service_has_no_journal_dependencies():
    from execution_plane_client.handlers import CommandRegistry
    from execution_plane_client.security import PeerConfig
    from execution_plane_transport_direct.queries import InMemoryCommandQueryProvider
    from execution_plane_transport_direct.service import build_direct_service

    async def certificate_auth(_request: object) -> str:
        return "ao"

    peer = PeerConfig(
        peer_id="ao",
        issuer="urn:ao",
        audience="urn:ao",
        role="controller",
        command_url="https://ao.test/v1/commands",
        query_base_url="https://ao.test/v1",
        grants={},
        verification_keys={},
        permitted_networks=["192.0.2.0/24"],
    )
    transport = AsyncMock()
    service = build_direct_service(
        local_id="ep",
        local_role="executor",
        command_url="https://ep.test/v1/commands",
        peers={"ao": peer},
        registry=CommandRegistry(),
        transport=transport,
        verifier=object(),
        certificate_auth=certificate_auth,
    )

    await service.start()
    await service.close()

    transport.aclose.assert_awaited_once()
    assert isinstance(service.queries, InMemoryCommandQueryProvider)
    assert not hasattr(service, "journal")
