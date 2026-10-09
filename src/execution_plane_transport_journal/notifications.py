"""Transactional hints for committed work; the journal remains authoritative."""

from __future__ import annotations

import asyncio
import hashlib
from typing import TYPE_CHECKING, Literal

import asyncpg  # type: ignore[import-untyped]
from sqlalchemy import func, select

from execution_plane_client.observers import NullObserver

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.observers import ExchangeObserver
    from execution_plane_transport_journal.schema import JournalSchema


def notification_channel(schema: JournalSchema) -> str:
    """Isolate journals sharing a database without exposing message identities."""
    table = schema.table
    identity = f"{table.schema or 'public'}.{table.name}".encode()
    return "exchange_journal_" + hashlib.sha256(identity).hexdigest()[:24]


async def notify_journal(session: AsyncSession, schema: JournalSchema, kind: Literal["delivery", "handling"]) -> None:
    """Publish only if the caller's outer transaction commits, including savepoints."""
    await session.execute(select(func.pg_notify(notification_channel(schema), kind)))


class JournalNotifications:
    """Wake local workers after commits in any process using this journal."""

    def __init__(
        self,
        schema: JournalSchema,
        sessions: Callable[[], AsyncSession],
        delivery: asyncio.Event,
        handling: asyncio.Event,
        *,
        observer: ExchangeObserver | None = None,
        health_seconds: float = 30,
        reconnect_seconds: float = 1,
    ) -> None:
        """Reserve one autocommit connection using participant connection settings."""
        self._channel = notification_channel(schema)
        self._sessions = sessions
        self._delivery = delivery
        self._handling = handling
        self._observer = observer or NullObserver()
        self._health_seconds = health_seconds
        self._reconnect_seconds = reconnect_seconds

    def _received(self, _connection: object, _pid: int, _channel: str, payload: str) -> None:
        if payload == "delivery":
            self._delivery.set()
        elif payload == "handling":
            self._handling.set()

    def _rescan(self) -> None:
        self._delivery.set()
        self._handling.set()

    async def _listen(self) -> None:
        # A dedicated connection must remain outside database transactions:
        # PostgreSQL defers notification delivery until the listener commits.
        async with self._sessions() as session:
            connection = await session.connection(execution_options={"isolation_level": "AUTOCOMMIT"})
            raw = await connection.get_raw_connection()
            driver = raw.driver_connection
            if not isinstance(driver, asyncpg.Connection):
                error = "Journal notifications require a PostgreSQL asyncpg connection"
                raise TypeError(error)
            disconnected = asyncio.Event()

            def terminated(_connection: object) -> None:
                disconnected.set()

            driver.add_termination_listener(terminated)
            try:
                async with asyncio.timeout(self._health_seconds):
                    await driver.add_listener(self._channel, self._received)
                # LISTEN is active before the scan, closing the startup/reconnect race.
                self._rescan()
                self._observer.record("journal.listener.ready", {})
                while not disconnected.is_set():
                    try:
                        await asyncio.wait_for(disconnected.wait(), self._health_seconds)
                    except TimeoutError:
                        async with asyncio.timeout(self._health_seconds):
                            await driver.fetchval("SELECT 1")
            finally:
                driver.remove_termination_listener(terminated)
                # Do not return a LISTEN registration or a stale socket to the pool.
                # Terminating only this dedicated connection also bounds cancellation.
                driver.terminate()
                await connection.invalidate()

    async def run(self) -> None:
        """Reconnect while independent worker timers keep durable recovery alive."""
        while True:
            try:
                await self._listen()
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - Notification failure must not stop durable recovery.
                self._observer.record("journal.listener.disconnected", {"error_type": type(error).__name__})
            self._rescan()
            await asyncio.sleep(self._reconnect_seconds)
