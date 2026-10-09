"""Certificate identity from verified TLS, combined with shared request assertions."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import HTTPException, Request

if TYPE_CHECKING:
    from collections.abc import Mapping

    from execution_plane_client.api import CertificateAuthenticator


def build_certificate_auth(common_names: Mapping[str, str]) -> CertificateAuthenticator:
    """Map operator-registered certificate common names to protocol peer IDs."""

    async def authenticate(request: Request) -> str:
        certificate = request.scope.get("extensions", {}).get("tls", {}).get("peercert", {})
        names = [
            str(value) for group in certificate.get("subject", ()) for kind, value in group if kind == "commonName"
        ]
        if len(names) != 1 or names[0] not in common_names:
            raise HTTPException(status_code=403, detail="Registered client certificate required")
        return common_names[names[0]]

    return authenticate
