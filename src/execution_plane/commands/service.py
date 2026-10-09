"""EP worker lifecycle around a runtime supplied by a transport package."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from execution_plane.work_store import WorkStore
    from execution_plane_client.api import CommandRuntime


class EPCommandService:
    """Supervise command transport workers without selecting an implementation."""

    def __init__(self, runtime: CommandRuntime) -> None:
        """Require the application composition root to provide its selected runtime."""
        self._tasks: list[asyncio.Task[None]] = []
        self._closed = False
        self.runtime = runtime

    def configure_store(self, store: WorkStore) -> None:
        """Install the transport-specific completion recorder before polling work."""
        recorder = self.runtime.work_completion_recorder
        if recorder is None:
            error = "The selected command runtime must provide a WorkItem completion recorder"
            raise RuntimeError(error)
        store.set_completion_recorder(recorder)

    async def run(self, store: WorkStore) -> None:
        """Run command transport work beside EP's provider worker."""
        self.configure_store(store)
        try:
            async with asyncio.TaskGroup() as group:
                self._tasks = [group.create_task(self.runtime.run())]
        finally:
            await self.close()

    async def close(self) -> None:
        """Stop runtime tasks and release the selected transport."""
        if self._closed:
            return
        self._closed = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.runtime.close()
