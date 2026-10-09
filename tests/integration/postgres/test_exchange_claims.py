"""Delivery state stays distinct from completion and stale claims are fenced."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from .exchange_helpers import journal_database
from .test_exchange_handshake import identity_for, receipt_handler


async def test_late_http_receipt_does_not_erase_completion_status(work_message):
    from execution_plane_client.handlers import HandlingResult
    from execution_plane_client.protocol import CommandStatus
    from execution_plane_transport_journal.claims import DeliveryOutcome, JournalClaims
    from execution_plane_transport_journal.completion import append_completion

    async with journal_database() as ao, journal_database() as ep:
        claims = JournalClaims(ao.store)
        now = datetime.now(UTC) + timedelta(seconds=1)
        async with ao.sessions.begin() as session:
            await ao.store.append_outbound(session, work_message)
            claim = (await claims.claim_due_delivery(session, uuid4(), now, 1))[0]
        async with ep.sessions.begin() as session:
            await receipt_handler(ep.store).receive(work_message, identity_for(work_message), session)
            completion = await append_completion(
                ep.store,
                session,
                work_message,
                HandlingResult(status=CommandStatus.SUCCEEDED, completion_required=True),
            )
        async with ao.sessions.begin() as session:
            await receipt_handler(ao.store, local_id="ao").receive(completion, identity_for(completion), session)
            changed = await claims.record_delivery(session, claim, DeliveryOutcome("receipt"), now)
        assert changed is True
        async with ao.sessions() as session:
            entry = await ao.store.get(session, work_message.scope, work_message.command_id, "outbound")
        assert entry.delivery_state == "sent"
        assert entry.command_status == CommandStatus.SUCCEEDED


async def test_expired_delivery_claim_is_fenced_and_202_stops_retries(work_message):
    from execution_plane_transport_journal.claims import DeliveryOutcome, JournalClaims

    async with journal_database() as db:
        claims = JournalClaims(db.store, lease_seconds=5)
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
            assert await claims.claim_due_delivery(session, uuid4(), now + timedelta(seconds=6), 1) == []
