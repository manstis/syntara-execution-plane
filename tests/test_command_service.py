"""EP delivery uses the shared protocol runtime and configures the work recorder."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from execution_plane.commands.service import EPCommandService


async def test_shared_runtime_is_closed_and_recorder_is_installed():
    runtime = SimpleNamespace(journal=object(), sessions=MagicMock(), close=AsyncMock())
    store = MagicMock()
    delivery = EPCommandService(SimpleNamespace(), runtime=runtime)
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

    runtime = SimpleNamespace(journal=object(), sessions=MagicMock(), close=AsyncMock(), run=poll)
    delivery = EPCommandService(SimpleNamespace(), runtime=runtime)
    task = asyncio.create_task(delivery.run(MagicMock()))
    await started.wait()
    await delivery.close()
    await asyncio.wait_for(task, timeout=1)
    assert len(stopped) == 1
    runtime.close.assert_awaited_once()
