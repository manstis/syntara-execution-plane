"""Query responses scope identity and restrict private outcome recovery."""

import pytest

from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import CommandStatus
from execution_plane_transport_journal.completion import append_completion

from .exchange_helpers import journal_database
from .test_exchange_handshake import identity_for, receipt_handler


async def test_private_child_is_only_returned_to_its_recipient(work_message):
    from execution_plane_client.errors import CommandNotFound
    from execution_plane_transport_journal.queries import JournalSessionCommandQueryProvider

    async with journal_database() as db:
        async with db.sessions.begin() as session:
            await receipt_handler(db.store).receive(work_message, identity_for(work_message), session)
            completion = await append_completion(
                db.store,
                session,
                work_message,
                HandlingResult(status=CommandStatus.SUCCEEDED, result={"private": "result"}, completion_required=True),
            )
        queries = JournalSessionCommandQueryProvider(db.store)
        async with db.sessions() as session:
            normal = await queries.children(session, work_message.scope, work_message.command_id, 0, 50, peer_id="ao")
            recovered = await queries.children(
                session, work_message.scope, work_message.command_id, 0, 50, peer_id="ao", include_recovery=True
            )
            with pytest.raises(CommandNotFound):
                await queries.get(session, work_message.scope, work_message.command_id, peer_id="other")
        assert normal.items[0].recovery_message is None
        assert "private" not in normal.model_dump_json()
        assert recovered.items[0].recovery_message == completion
        assert recovered.items[0].parent_sequence == 1
