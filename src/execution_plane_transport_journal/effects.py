"""Encrypted reconstruction of local effects; never part of the wire protocol."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID  # noqa: TC003 - Pydantic resolves the stored model annotations at runtime.

from execution_plane_client.errors import ProtocolConflict
from execution_plane_client.handlers import PreparedEffect
from execution_plane_client.protocol import PositiveInt, WireModel

if TYPE_CHECKING:
    from execution_plane_client.crypto import PayloadCipher


class StoredEffect(WireModel):
    """Private encrypted effect snapshot retained by the journal."""

    entry_id: UUID
    command_id: UUID
    domain_generation: PositiveInt
    context: dict[str, object]


def restore_effect(cipher: PayloadCipher, ciphertext: str, entry_id: UUID, command_id: UUID) -> PreparedEffect:
    """Authenticate an effect's row identity before reconstructing it."""
    data = cipher.decrypt(ciphertext, associated_data=b"prepared-effect-v1:" + entry_id.bytes)
    value = StoredEffect.model_validate_json(data)
    if value.entry_id != entry_id or value.command_id != command_id:
        raise ProtocolConflict
    return PreparedEffect(value.entry_id, value.command_id, value.domain_generation, value.context)
