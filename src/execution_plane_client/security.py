"""Short-lived ES256 assertions bound to exact envelopes and scoped peer grants."""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import jwt
from pydantic import Field, field_validator

from execution_plane_client.crypto import canonical_bytes
from execution_plane_client.errors import UnauthorizedPeer
from execution_plane_client.protocol import Command, CommandScope, MessageEnvelope, WireModel

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

_MAX_ASSERTION_SECONDS = 60
_CLOCK_SKEW_SECONDS = 5
_MAX_TOKEN_BYTES = 8192


class PeerGrant(WireModel):
    """Commands and projects a registered peer may act on for one initiating client."""

    project_ids: frozenset[UUID] | None
    permissions: frozenset[str]


class PeerConfig(WireModel):
    """Registered routes, signing trust and explicit outbound network allowance."""

    peer_id: str
    issuer: str
    audience: str
    command_url: str
    query_base_url: str
    grants: dict[str, PeerGrant]
    verification_keys: dict[str, str] = Field(repr=False)
    permitted_networks: list[str]
    role: str = "executor"

    @field_validator("command_url", "query_base_url")
    @classmethod
    def check_url(cls, value: str) -> str:
        """Apply the same strict HTTPS routing rules used on envelopes."""
        return Command.check_url(value)


@dataclass(frozen=True)
class AuthenticatedIdentity:
    """Verified peer and initiating client/project scope."""

    peer_id: str
    scope: CommandScope
    scopes: frozenset[str]
    role: str = "controller"


def _now() -> datetime:
    return datetime.now(UTC)


class AssertionSigner:
    """Mint fresh transport credentials without changing persisted message identity."""

    def __init__(
        self,
        *,
        local_id: str,
        issuer: str,
        key_id: str,
        private_key: str,
        peers: Mapping[str, PeerConfig],
        clock: Callable[[], datetime] = _now,
        lifetime_seconds: int = 30,
    ) -> None:
        """Use participant-owned signing identity and explicit destination audiences."""
        if not 1 <= lifetime_seconds <= _MAX_ASSERTION_SECONDS:
            error = "Assertion lifetime must be between 1 and 60 seconds"
            raise ValueError(error)
        self.local_id = local_id
        self.issuer = issuer
        self._key_id = key_id
        self._key = private_key
        self._peers = dict(peers)
        self._clock = clock
        self._lifetime = lifetime_seconds

    def _claims(self, peer_id: str, scope: CommandScope, permission: str) -> dict[str, object]:
        peer = self._peers.get(peer_id)
        if peer is None:
            raise UnauthorizedPeer
        now = int(self._clock().timestamp())
        return {
            "iss": self.issuer,
            "sub": self.local_id,
            "aud": peer.audience,
            "iat": now,
            "exp": now + self._lifetime,
            "jti": str(uuid4()),
            "scope": scope.model_dump(mode="json"),
            "permissions": [permission],
        }

    def _encode(self, claims: dict[str, object]) -> str:
        return jwt.encode(claims, self._key, algorithm="ES256", headers={"kid": self._key_id})

    def sign(self, message: MessageEnvelope) -> str:
        """Bind all immutable payload, identity, routing, policy and sequence fields."""
        if message.source_id != self.local_id:
            raise UnauthorizedPeer
        claims = self._claims(message.destination_id, message.scope, message.command.command_type)
        claims.update(
            command_id=str(message.command_id),
            message_id=str(message.message_id),
            envelope_sha256=hashlib.sha256(canonical_bytes(message)).hexdigest(),
        )
        return self._encode(claims)

    def sign_query(self, peer_id: str, scope: CommandScope, path: str) -> str:
        """Bind query credentials to the GET request target and authorized scope."""
        permission = "capabilities:read" if path.endswith("/capabilities") else "commands:read"
        claims = self._claims(peer_id, scope, permission)
        claims.update(method="GET", path=path)
        return self._encode(claims)


class AssertionVerifier:
    """Verify signatures and local grants without trusting sender-selected claims."""

    def __init__(
        self,
        *,
        local_id: str,
        audience: str,
        command_url: str,
        peers: Mapping[str, PeerConfig],
        clock: Callable[[], datetime] = _now,
    ) -> None:
        """Accept explicitly configured verification key IDs, including rotation overlap."""
        self.local_id = local_id
        self._audience = audience
        self._command_url = command_url
        self._peers = dict(peers)
        self._clock = clock

    def _decode(self, token: str, peer_id: str) -> tuple[dict[str, object], PeerConfig]:
        try:
            if len(token) > _MAX_TOKEN_BYTES:
                raise UnauthorizedPeer
            peer = self._peers[peer_id]
            header = jwt.get_unverified_header(token)
            key = peer.verification_keys[header["kid"]]
            claims = jwt.decode(
                token,
                key,
                algorithms=["ES256"],
                audience=self._audience,
                issuer=peer.issuer,
                options={
                    "require": ["iss", "sub", "aud", "iat", "exp", "jti", "scope", "permissions"],
                    "verify_exp": False,
                    "verify_iat": False,
                    "verify_nbf": False,
                },
            )
            issued, expiry = claims["iat"], claims["exp"]
            now = self._clock().timestamp()
            if (
                type(issued) is not int
                or type(expiry) is not int
                or not 0 < expiry - issued <= _MAX_ASSERTION_SECONDS
                or issued > now + _CLOCK_SKEW_SECONDS
                or expiry < now - _CLOCK_SKEW_SECONDS
                or claims["sub"] != peer_id
            ):
                raise UnauthorizedPeer
        except (jwt.PyJWTError, ValueError, TypeError, KeyError):
            raise UnauthorizedPeer from None
        return claims, peer

    def _authorize(self, claims: dict[str, object], peer: PeerConfig, permission: str) -> AuthenticatedIdentity:
        try:
            scope = CommandScope.model_validate(claims["scope"])
            grant = peer.grants[scope.client_id]
        except (ValueError, TypeError, KeyError):
            raise UnauthorizedPeer from None
        if claims["permissions"] != [permission] or permission not in grant.permissions:
            raise UnauthorizedPeer
        if (
            permission != "capabilities:read"
            and grant.project_ids is not None
            and (scope.all_projects or scope.project_id not in grant.project_ids)
        ):
            raise UnauthorizedPeer
        return AuthenticatedIdentity(peer.peer_id, scope, frozenset({permission}), peer.role)

    def verify(self, token: str, message: MessageEnvelope) -> AuthenticatedIdentity:
        """Require exact signed envelope identity and registered bilateral routes."""
        claims, peer = self._decode(token, message.source_id)
        identity = self._authorize(claims, peer, message.command.command_type)
        digest = claims.get("envelope_sha256")
        if (
            message.destination_id != self.local_id
            or identity.scope != message.scope
            or message.command.target_url != self._command_url
            or message.command.callback_url != peer.command_url
            or claims.get("command_id") != str(message.command_id)
            or claims.get("message_id") != str(message.message_id)
            or not isinstance(digest, str)
            or not hmac.compare_digest(digest, hashlib.sha256(canonical_bytes(message)).hexdigest())
        ):
            raise UnauthorizedPeer
        return identity

    def verify_query(self, token: str, path: str) -> AuthenticatedIdentity:
        """Use unverified subject only to select configured trust, then authenticate."""
        try:
            if len(token) > _MAX_TOKEN_BYTES:
                raise UnauthorizedPeer
            untrusted = jwt.decode(token, options={"verify_signature": False})
            peer_id = untrusted["sub"]
            if not isinstance(peer_id, str):
                raise UnauthorizedPeer
        except (jwt.PyJWTError, ValueError, TypeError, KeyError):
            raise UnauthorizedPeer from None
        claims, peer = self._decode(token, peer_id)
        if claims.get("method") != "GET" or claims.get("path") != path:
            raise UnauthorizedPeer
        permission = "capabilities:read" if path.endswith("/capabilities") else "commands:read"
        return self._authorize(claims, peer, permission)
