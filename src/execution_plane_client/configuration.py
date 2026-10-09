"""Explicit file-backed peer trust configuration for participants."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from pydantic import Field

from execution_plane_client.protocol import WireModel
from execution_plane_client.security import PeerConfig


class PeerRegistration(PeerConfig):
    """Operator trust entry; public-key files are resolved relative to this file."""

    certificate_common_names: list[str]
    verification_keys: dict[str, str] = Field(default_factory=dict, repr=False)
    verification_key_paths: dict[str, str] = Field(default_factory=dict)


class TrustFile(WireModel):
    """All registered peers, including the local participant's own identity."""

    peers: list[PeerRegistration]


@dataclass(frozen=True)
class PeerTrust:
    """Validated protocol keys and independent TLS-to-peer identity mapping."""

    peers: dict[str, PeerConfig]
    certificate_names: dict[str, str]


def load_peer_trust(path: str | Path) -> PeerTrust:
    """Read only the explicitly supplied trust file; never inspect environment globals."""
    source = Path(path)
    registrations = TrustFile.model_validate(json.loads(source.read_text())).peers
    peers = {}
    certificates = {}
    for registration in registrations:
        keys = dict(registration.verification_keys)
        for key_id, filename in registration.verification_key_paths.items():
            keys[key_id] = (source.parent / filename).read_text()
        if (
            registration.peer_id in peers
            or not keys
            or len(set(registration.certificate_common_names)) != len(registration.certificate_common_names)
            or any(name in certificates for name in registration.certificate_common_names)
        ):
            error = "Peer identities, certificate names and verification keys must be explicit and unique"
            raise ValueError(error)
        peer = PeerConfig.model_validate(
            {
                **registration.model_dump(exclude={"certificate_common_names", "verification_key_paths"}),
                "verification_keys": keys,
            }
        )
        peers[peer.peer_id] = peer
        for name in registration.certificate_common_names:
            certificates[name] = peer.peer_id
    return PeerTrust(peers, certificates)
