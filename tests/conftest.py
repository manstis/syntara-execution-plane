"""Shared fixtures for execution-plane unit tests."""

from collections.abc import Generator

import pytest

from execution_plane.config import get_ep_settings


@pytest.fixture(autouse=True)
def ep_settings_env(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Set the minimum required env vars for EPSettings and clear lru_caches."""
    monkeypatch.setenv("EP_DATABASE_URL", "postgresql+asyncpg://user:password@localhost/test")
    monkeypatch.setenv("EP_CREDENTIAL_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    get_ep_settings.cache_clear()
    yield
    get_ep_settings.cache_clear()


@pytest.fixture
def security_case():
    """Create bilateral signed-peer configuration for client and transport tests."""
    from datetime import UTC, datetime
    from types import SimpleNamespace
    from uuid import UUID

    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec

    from execution_plane_client.security import AssertionSigner, AssertionVerifier, PeerConfig, PeerGrant

    key = ec.generate_private_key(ec.SECP256R1())
    private = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    public = (
        key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )
    grant = PeerGrant(
        project_ids={UUID(int=3)},
        permissions={"work.execute", "work.execute.completed", "commands:read", "capabilities:read"},
    )
    peers = {
        name: PeerConfig(
            peer_id=name,
            issuer=f"urn:{name}",
            audience=f"urn:{name}",
            command_url=f"https://{name}.test/v1/commands",
            query_base_url=f"https://{name}.test/v1",
            grants={"ao": grant},
            verification_keys={"k1": public, "old": public},
            permitted_networks=["192.0.2.0/24"],
        )
        for name in ("ao", "ep")
    }
    now = datetime(2026, 10, 9, 12, tzinfo=UTC)
    signer = AssertionSigner(
        local_id="ao", issuer="urn:ao", key_id="k1", private_key=private, peers=peers, clock=lambda: now
    )
    verifier = AssertionVerifier(
        local_id="ep", audience="urn:ep", command_url=peers["ep"].command_url, peers=peers, clock=lambda: now
    )
    return SimpleNamespace(signer=signer, verifier=verifier, private=private, public=public, peers=peers, now=now)


@pytest.fixture
def message():
    """Provide a valid command envelope for shared client and transport tests."""
    from uuid import UUID

    from execution_plane_client.protocol import MessageEnvelope

    return MessageEnvelope.model_validate(
        {
            "message_id": str(UUID(int=1)),
            "message_type": "command.submit",
            "command_id": str(UUID(int=2)),
            "sequence": 0,
            "source_id": "ao",
            "destination_id": "ep",
            "scope": {"client_id": "ao", "project_id": str(UUID(int=3)), "all_projects": False},
            "command": {
                "command_id": str(UUID(int=2)),
                "parent_command_id": None,
                "command_type": "work.execute",
                "command_revision": 1,
                "target_url": "https://ep.test/v1/commands",
                "callback_url": "https://ao.test/v1/commands",
                "payload": {"invocation": {"script": "print(42)"}, "limit": 1},
                "occurred_at": "2026-10-09T10:00:00Z",
                "parent_sequence": None,
                "execution_policy": {"timeout_seconds": 30, "max_attempts": 1},
            },
        }
    )
