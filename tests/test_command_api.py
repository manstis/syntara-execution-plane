"""The EP facade exposes one command ingress and requires transport identity."""

import pytest
from fastapi import HTTPException, Request

from execution_plane.api.main import create_app


def test_only_command_ingress_mutates():
    schema = create_app().openapi()
    mutations = {
        (path, method.upper())
        for path, operations in schema["paths"].items()
        for method in operations
        if method in {"post", "put", "patch", "delete"}
    }
    assert mutations == {("/v1/commands", "POST")}
    assert "requestBody" in schema["paths"]["/v1/commands"]["post"]
    assert "/v1/clusters/{resource_id}" in schema["paths"]


async def test_certificate_identity_comes_only_from_verified_transport():
    from execution_plane.api.auth import build_certificate_auth

    authenticate = build_certificate_auth({"ao.service": "ao"})
    request = Request({"type": "http", "headers": [(b"x-client-cn", b"ao.service")]})
    with pytest.raises(HTTPException) as rejected:
        await authenticate(request)
    assert rejected.value.status_code == 403
    request = Request(
        {
            "type": "http",
            "headers": [],
            "extensions": {
                "tls": {
                    "peercert": {
                        "subject": ((("commonName", "ao.service"),),),
                    }
                }
            },
        }
    )
    assert await authenticate(request) == "ao"
