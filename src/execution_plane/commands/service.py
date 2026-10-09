"""EP lifecycle integration for the shared durable command handshake."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane.commands.runtime import build_ep_runtime
from execution_plane.commands.work import EPCompletionRecorder

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from execution_plane.config import EPSettings
    from execution_plane.work_store import WorkStore
    from execution_plane_client.service import MediatorService


class EPCommandService:
    """Supervise journal workers and domain observation; provider observations produce journal commands."""

    def __init__(self, settings: EPSettings, *, runtime: MediatorService | None = None) -> None:
        """Own a background pool unless the participant runtime is explicitly injected."""
        self._tasks: list[asyncio.Task[None]] = []
        self._closed = False
        self._engine: AsyncEngine | None = None
        if runtime is None:
            self._engine = create_async_engine(settings.database_url)
            sessions = async_sessionmaker(self._engine, expire_on_commit=False)
            runtime = build_ep_runtime(settings, sessions)
        self.runtime = runtime

    def configure_store(self, store: WorkStore) -> None:
        """Install completion recording before any provider or claim polling starts."""
        store.set_completion_recorder(EPCompletionRecorder(self.runtime.journal))

    async def run(self, store: WorkStore) -> None:
        """Run delivery, handling, callback recovery and domain observation together."""
        self.configure_store(store)
        try:
            async with asyncio.TaskGroup() as group:
                self._tasks = [group.create_task(self.runtime.run())]
        finally:
            await self.close()

    async def close(self) -> None:
        """Close transport and the owned pool after worker task cancellation."""
        if self._closed:
            return
        self._closed = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.runtime.close()
        if self._engine is not None:
            await self._engine.dispose()
