"""Assertions bind authenticated scope to the complete persisted envelope."""

from uuid import UUID

import jwt
import pytest


def test_valid_assertion_and_fresh_retry(security_case, message):
    identity = security_case.verifier.verify(security_case.signer.sign(message), message)
    assert identity.peer_id == "ao"
    assert identity.scope.project_id == UUID(int=3)
    assert identity.scopes == frozenset({"work.execute"})
    assert security_case.signer.sign(message) != security_case.signer.sign(message)


@pytest.mark.parametrize("change", ["payload", "route", "sequence", "project", "client"])
def test_assertion_rejects_tampered_envelope(security_case, message, change):
    from execution_plane_client.errors import UnauthorizedPeer

    token = security_case.signer.sign(message)
    if change == "payload":
        message.command.payload["limit"] = 999
    elif change == "route":
        message = message.model_copy(
            update={
                "command": message.command.model_copy(update={"callback_url": "https://unregistered.test/v1/commands"})
            }
        )
    elif change == "sequence":
        message = message.model_copy(update={"sequence": 2})
    else:
        field, value = ("project_id", UUID(int=99)) if change == "project" else ("client_id", "another")
        message = message.model_copy(update={"scope": message.scope.model_copy(update={field: value})})
    with pytest.raises(UnauthorizedPeer):
        security_case.verifier.verify(token, message)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("iss", "urn:other"),
        ("aud", "urn:other"),
        ("sub", "other"),
        ("iat", 9999999999),
        ("exp", 1),
        ("exp", 9999999999),
        ("permissions", ["commands:read"]),
    ],
)
def test_even_signed_claims_obey_peer_policy(security_case, message, field, value):
    from execution_plane_client.errors import UnauthorizedPeer

    claims = jwt.decode(security_case.signer.sign(message), options={"verify_signature": False})
    claims[field] = value
    token = jwt.encode(claims, security_case.private, algorithm="ES256", headers={"kid": "k1"})
    with pytest.raises(UnauthorizedPeer):
        security_case.verifier.verify(token, message)


def test_key_overlap_and_unknown_key(security_case, message):
    from execution_plane_client.errors import UnauthorizedPeer

    claims = jwt.decode(security_case.signer.sign(message), options={"verify_signature": False})
    old = jwt.encode(claims, security_case.private, algorithm="ES256", headers={"kid": "old"})
    assert security_case.verifier.verify(old, message).peer_id == "ao"
    unknown = jwt.encode(claims, security_case.private, algorithm="ES256", headers={"kid": "unknown"})
    with pytest.raises(UnauthorizedPeer):
        security_case.verifier.verify(unknown, message)


def test_query_assertion_cannot_be_replayed_for_another_path(security_case, message):
    from execution_plane_client.errors import UnauthorizedPeer

    path = f"/v1/commands/{message.command_id}"
    token = security_case.signer.sign_query("ep", message.scope, path)
    assert security_case.verifier.verify_query(token, path).peer_id == "ao"
    with pytest.raises(UnauthorizedPeer):
        security_case.verifier.verify_query(token, "/v1/commands/another")


def test_validly_signed_foreign_project_is_still_unauthorized(security_case, message):
    from execution_plane_client.errors import UnauthorizedPeer

    foreign = message.model_copy(update={"scope": message.scope.model_copy(update={"project_id": UUID(int=99)})})
    with pytest.raises(UnauthorizedPeer):
        security_case.verifier.verify(security_case.signer.sign(foreign), foreign)
