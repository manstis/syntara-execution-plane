"""Submission participates in a caller transaction and never performs HTTP."""

import pytest

from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.handlers import CommandRegistry, HandlingResult

from .exchange_helpers import journal_database


async def test_client_submit_does_not_commit_or_contact_peer(work_message):
    from execution_plane_client.client import MediatorClient
    from execution_plane_client.security import PeerConfig

    class Handler:
        async def handle(self, message, context, session) -> HandlingResult:
            return HandlingResult()

    class NoNetwork:
        async def get_capabilities(self, peer_id, scope=None) -> None:
            pytest.fail("Submission must not inspect remote capabilities")

        async def send(self, message) -> None:
            pytest.fail("Submission must not perform HTTP")

    async with journal_database() as db:
        catalog = CommandCatalog()
        registry = CommandRegistry()
        registry.register(catalog.definition("work.execute.completed", 1), Handler())
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
            db.store,
            catalog,
            registry,
            NoNetwork(),
            local_id="ao",
            local_role="controller",
            command_url="https://ao.test/v1/commands",
            peers={"ep": peer},
        )
        async with db.sessions() as session:
            await client.submit(work_message, session)
            await session.rollback()
        assert await db.read_rows() == []
