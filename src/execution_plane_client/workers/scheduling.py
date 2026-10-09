"""Bounded, continuously replenished work driven by commit hints and deadlines."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from datetime import datetime


async def run_claimed[T](
    claim: Callable[[int], Awaitable[list[T]]],
    process: Callable[[T], Awaitable[None]],
    next_due: Callable[[], Awaitable[datetime | None]],
    wakeup: asyncio.Event,
    *,
    clock: Callable[[], datetime],
    limit: int,
    poll_seconds: float,
) -> None:
    """Claim only free capacity, without losing a wakeup racing an empty scan."""
    active: set[asyncio.Task[None]] = set()

    def completed(task: asyncio.Task[None]) -> None:
        active.discard(task)
        wakeup.set()

    async def run(item: T) -> None:
        await process(item)

    async with asyncio.TaskGroup() as group:
        while True:
            # Clear before querying: a later commit sets the event for the next pass.
            wakeup.clear()
            capacity = limit - len(active)
            if capacity:
                claims = await claim(capacity)
                for item in claims:
                    task = group.create_task(run(item))
                    active.add(task)
                    task.add_done_callback(completed)
                if claims:
                    continue
            delay = poll_seconds
            if len(active) < limit:
                due = await next_due()
                if due is not None:
                    # A due row locked by another transaction must not cause a spin.
                    delay = min(delay, max(0.05, (due - clock()).total_seconds()))
            with suppress(TimeoutError):
                await asyncio.wait_for(wakeup.wait(), delay)
