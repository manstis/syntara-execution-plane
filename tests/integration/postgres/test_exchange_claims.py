"""Concurrent workers and delayed responses cannot overwrite newer responsibility."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import select, update

from .exchange_helpers import journal_database
from .test_exchange_handshake import identity_for, receipt_handler


async def test_late_transport_response_cannot_undo_acceptance(work_message):
    from execution_plane_client.journal.claims import DeliveryOutcome, JournalClaims

    async with journal_database() as ao, journal_database() as ep:
        claims = JournalClaims(ao.store)
        now = datetime.now(UTC)
        async with ao.sessions.begin() as session:
            await ao.store.append_outbound(session, work_message)
            claim = (await claims.claim_due_delivery(session, uuid4(), now + timedelta(seconds=1), 1))[0]
        async with ep.sessions.begin() as session:
            await receipt_handler(ep.store).receive(work_message, identity_for(work_message), session)
            table = ep.store.schema.table
            entry_id = await session.scalar(select(table.c.entry_id).where(table.c.command_type == "command.accepted"))
            acceptance = await ep.store.load_message(session, entry_id)
        async with ao.sessions.begin() as session:
            await receipt_handler(ao.store, local_id="ao").receive(acceptance, identity_for(acceptance), session)
            changed = await claims.record_delivery(session, claim, DeliveryOutcome("receipt"), now)
        assert changed is False
        async with ao.sessions() as session:
            entry = await ao.store.get(session, work_message.scope, work_message.command_id, "outbound")
        assert entry.delivery_state == "acknowledged"


async def test_expired_claim_is_fenced_and_sent_without_ack_is_retried(work_message):
    from execution_plane_client.journal.claims import DeliveryOutcome, JournalClaims

    async with journal_database() as db:
        claims = JournalClaims(db.store, lease_seconds=5, acceptance_seconds=5)
        now = datetime.now(UTC) + timedelta(seconds=1)
        async with db.sessions.begin() as session:
            await db.store.append_outbound(session, work_message)
            old = (await claims.claim_due_delivery(session, uuid4(), now, 1))[0]
        async with db.sessions.begin() as session:
            assert await claims.claim_due_delivery(session, uuid4(), now, 1) == []
            newer = (await claims.claim_due_delivery(session, uuid4(), now + timedelta(seconds=6), 1))[0]
            assert newer.generation == old.generation + 1
            assert await claims.record_delivery(session, old, DeliveryOutcome("receipt"), now) is False
            assert await claims.record_delivery(session, newer, DeliveryOutcome("receipt"), now) is True
        async with db.sessions.begin() as session:
            retry = (await claims.claim_due_delivery(session, uuid4(), now + timedelta(seconds=6), 1))[0]
        assert retry.message.message_id == work_message.message_id


async def test_duplicate_receipt_reactivates_the_same_delivered_acceptance(work_message):
    async with journal_database() as db:
        receiver = receipt_handler(db.store)
        async with db.sessions.begin() as session:
            await receiver.receive(work_message, identity_for(work_message), session)
            table = db.store.schema.table
            await session.execute(
                update(table)
                .where(table.c.command_type == "command.accepted")
                .values(delivery_state="acknowledged", delivery_due_at=None)
            )
        async with db.sessions.begin() as session:
            await receiver.receive(work_message, identity_for(work_message), session)
        acknowledgements = [row for row in await db.read_rows() if row["command_type"] == "command.accepted"]
        assert len(acknowledgements) == 1
        assert acknowledgements[0]["delivery_state"] == "queued"
