"""Transport-neutral delivery receipt contracts."""

from uuid import UUID

import pytest
from pydantic import ValidationError


def test_dispatch_receipt_preserves_message_id_and_delivery_state():
    from execution_plane_client.transport import DispatchReceipt, DispatchState

    message_id = UUID(int=1)

    queued = DispatchReceipt(message_id=message_id, state="queued")
    delivered = DispatchReceipt(message_id=message_id, state=DispatchState.DELIVERED)

    assert queued.message_id == message_id
    assert queued.state is DispatchState.QUEUED
    assert delivered.message_id == message_id
    assert delivered.state is DispatchState.DELIVERED


def test_dispatch_receipt_rejects_unknown_delivery_state():
    from execution_plane_client.transport import DispatchReceipt

    with pytest.raises(ValidationError):
        DispatchReceipt(message_id=UUID(int=1), state="accepted")
