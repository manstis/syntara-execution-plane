"""Canonical wire identity and injected authenticated encryption at rest."""

from __future__ import annotations

import base64
import binascii
import json
import os
import re
from typing import TYPE_CHECKING, Protocol

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from execution_plane_client.errors import InvalidCiphertext
from execution_plane_client.protocol import MessageEnvelope

_NONCE_BYTES = 12
_KEY_BYTES = 32

if TYPE_CHECKING:
    from collections.abc import Mapping


def canonical_bytes(message: MessageEnvelope) -> bytes:
    """Snapshot and validate the complete immutable envelope before hashing."""
    validated = MessageEnvelope.model_validate(message.model_dump(mode="python"))
    return json.dumps(
        validated.model_dump(mode="json"),
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


class PayloadCipher(Protocol):
    """Participant-owned keys with identity-bound authenticated encryption."""

    def encrypt(self, data: bytes, *, associated_data: bytes) -> str:
        """Encrypt immutable bytes using the active participant key."""
        ...

    def decrypt(self, value: str, *, associated_data: bytes) -> bytes:
        """Authenticate and decrypt, including configured retiring keys."""
        ...


class AESGCMCipher:
    """AES-256-GCM with random nonces and explicit key identifiers."""

    def __init__(self, *, active_key_id: str, keys: Mapping[str, bytes]) -> None:
        """Copy key material without reading environment or application settings."""
        if active_key_id not in keys or any(
            not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", kid) or len(key) != _KEY_BYTES for kid, key in keys.items()
        ):
            message = "Encryption requires named AES-256 keys and an installed active key"
            raise ValueError(message)
        self._active_key_id = active_key_id
        self._keys = {kid: AESGCM(bytes(key)) for kid, key in keys.items()}

    def encrypt(self, data: bytes, *, associated_data: bytes) -> str:
        """Bind ciphertext to its exact journal identity."""
        nonce = os.urandom(_NONCE_BYTES)
        encrypted = self._keys[self._active_key_id].encrypt(nonce, data, associated_data)
        encoded = base64.urlsafe_b64encode(nonce + encrypted).decode("ascii")
        return f"v1.{self._active_key_id}.{encoded}"

    def decrypt(self, value: str, *, associated_data: bytes) -> bytes:
        """Reject unknown keys, corruption, and cross-record ciphertext copying."""
        try:
            version, kid, encoded = value.split(".", 2)
            if version != "v1":
                raise ValueError  # noqa: TRY301 - Normalize all invalid formats to a safe integrity error.
            encrypted = base64.b64decode(encoded, altchars=b"-_", validate=True)
            return self._keys[kid].decrypt(encrypted[:_NONCE_BYTES], encrypted[_NONCE_BYTES:], associated_data)
        except (ValueError, KeyError, InvalidTag, binascii.Error):
            message = "Encrypted journal content failed authentication"
            raise InvalidCiphertext(message) from None
