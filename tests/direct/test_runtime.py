"""Direct factory composes the shared receiver without Journal storage."""

from unittest.mock import AsyncMock

import pytest


async def test_direct_factory_builds_and_closes_a_client_runtime(security_case, monkeypatch):
    from execution_plane_client.configuration import PeerTrust
    from execution_plane_client.handlers import CommandRegistry
    from execution_plane_transport_direct import runtime
    from execution_plane_transport_direct.runtime import DirectRuntimeFactory

    transport = AsyncMock()
    monkeypatch.setattr(runtime, "HttpCommandTransport", lambda **_kwargs: transport)

    async def certificate_auth(_request: object) -> str:
        return "ep"

    factory = DirectRuntimeFactory(
        trust=PeerTrust(security_case.peers, {"ao": "ao", "ep": "ep"}),
        local_peer_id="ep",
        expected_role="executor",
        signing_key_id="k1",
        signing_private_key=security_case.private,
        tls=runtime.TLSConfig("unused-ca", "unused-cert", "unused-key"),
        certificate_auth=certificate_auth,
    )
    service = factory.build(CommandRegistry())

    assert service.create_router().prefix == "/v1"
    assert not hasattr(service, "journal")
    await service.start()
    await service.close()
    transport.aclose.assert_awaited_once()


def test_direct_factory_validates_local_peer_role(security_case):
    from execution_plane_client.configuration import PeerTrust
    from execution_plane_transport_direct.runtime import DirectRuntimeFactory, TLSConfig

    async def certificate_auth(_request: object) -> str:
        return "ep"

    with pytest.raises(ValueError, match="must have role"):
        DirectRuntimeFactory(
            trust=PeerTrust(security_case.peers, {"ao": "ao", "ep": "ep"}),
            local_peer_id="ep",
            expected_role="controller",
            signing_key_id="k1",
            signing_private_key=security_case.private,
            tls=TLSConfig("unused-ca", "unused-cert", "unused-key"),
            certificate_auth=certificate_auth,
        )
