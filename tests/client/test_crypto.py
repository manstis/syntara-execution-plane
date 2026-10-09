"""Envelope identity and at-rest integrity are independent of encryption nonce."""

import pytest


def test_canonical_envelope_hash_covers_routing_and_nested_values(message):
    from execution_plane_client.crypto import canonical_bytes

    original = canonical_bytes(message)
    assert original == canonical_bytes(message.model_copy(deep=True))
    changed = message.model_copy(
        update={"command": message.command.model_copy(update={"target_url": "https://other.test/v1/commands"})}
    )
    assert canonical_bytes(changed) != original
    message.command.payload["invocation"]["script"] = "print(43)"
    assert canonical_bytes(message) != original


def test_cipher_binds_identity_and_supports_key_rotation():
    from execution_plane_client.crypto import AESGCMCipher
    from execution_plane_client.errors import InvalidCiphertext

    old = AESGCMCipher(active_key_id="old", keys={"old": b"x" * 32})
    rotated = AESGCMCipher(active_key_id="new", keys={"old": b"x" * 32, "new": b"y" * 32})
    token = old.encrypt(b"private-test-value", associated_data=b"message-1")
    assert "private-test-value" not in token
    assert old.encrypt(b"private-test-value", associated_data=b"message-1") != token
    assert rotated.decrypt(token, associated_data=b"message-1") == b"private-test-value"
    with pytest.raises(InvalidCiphertext):
        rotated.decrypt(token, associated_data=b"message-2")
    with pytest.raises(InvalidCiphertext):
        rotated.decrypt(token[:-5] + "AAAAA", associated_data=b"message-1")


def test_journal_metadata_is_participant_owned():
    from sqlmodel import SQLModel

    from execution_plane_client.journal.schema import build_journal_schema

    existing = set(SQLModel.metadata.tables)
    ao = build_journal_schema(None)
    ep = build_journal_schema("execution_plane")
    assert ao.metadata is not ep.metadata
    assert ao.table.schema is None
    assert ep.table.schema == "execution_plane"
    assert set(SQLModel.metadata.tables) == existing
