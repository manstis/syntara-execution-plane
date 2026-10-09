"""Peer trust rejects ambiguous identities and loads explicit rotating key IDs."""

import json

import pytest

from execution_plane_client.configuration import load_peer_trust


def test_key_files_resolve_relative_to_trust_file_and_duplicate_certificates_fail(tmp_path, security_case):
    peer = security_case.peers["ep"].model_dump(mode="json")
    key = next(iter(peer.pop("verification_keys").values()))
    (tmp_path / "public.pem").write_text(key)
    peer.update(
        certificate_common_names=["ep.service"], verification_key_paths={"old": "public.pem", "current": "public.pem"}
    )
    filename = tmp_path / "trust.json"
    filename.write_text(json.dumps({"peers": [peer]}))
    trust = load_peer_trust(filename)
    assert trust.peers["ep"].verification_keys == {"old": key, "current": key}
    other = {**peer, "peer_id": "another"}
    filename.write_text(json.dumps({"peers": [peer, other]}))
    with pytest.raises(ValueError, match="unique"):
        load_peer_trust(filename)


def test_client_route_uses_registered_peer_urls(security_case):
    from unittest.mock import Mock

    from execution_plane_client.client import MediatorClient
    from execution_plane_client.protocol import CommandScope

    scope = CommandScope(client_id="ao", project_id=None, all_projects=True)
    client = MediatorClient(
        Mock(),
        Mock(),
        Mock(),
        local_id="ao",
        local_role="controller",
        command_url="https://ao.test/v1/commands",
        peers=security_case.peers,
    )
    route = client.route(scope, peer_id="ep")
    assert route.scope == scope
    assert route.target_url == security_case.peers["ep"].command_url
    assert route.callback_url == "https://ao.test/v1/commands"
    assert route.source_id == "ao"
    assert route.destination_id == "ep"
