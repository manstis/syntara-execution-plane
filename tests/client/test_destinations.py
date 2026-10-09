"""Registered routes and DNS pinning prevent arbitrary callback connections."""

import httpx
import pytest


@pytest.mark.parametrize(
    "url",
    [
        "http://ep.test/v1/commands",
        "https://ep.test/v1/commands?token=secret",
        "https://ep.test/v1/commands#fragment",
        "https://user:password@ep.test/v1/commands",
        "https://unregistered.test/v1/commands",
        "https://ep.test/other",
    ],
)
def test_only_registered_command_route_is_accepted(security_case, url):
    from execution_plane_client.destinations import DestinationPolicy
    from execution_plane_client.errors import UnsafeDestination

    with pytest.raises(UnsafeDestination):
        DestinationPolicy(security_case.peers).validate(url, "ep")


async def test_mixed_dns_answers_are_rejected(security_case):
    from execution_plane_client.destinations import DestinationPolicy
    from execution_plane_client.errors import UnsafeDestination

    async def resolve(host, port) -> list[str]:
        return ["192.0.2.10", "127.0.0.1"]

    policy = DestinationPolicy(security_case.peers, resolver=resolve)
    with pytest.raises(UnsafeDestination):
        await policy.resolve(policy.validate("https://ep.test/v1/commands", "ep"))


async def test_connection_uses_checked_address_and_original_tls_name(security_case):
    from execution_plane_client.destinations import DestinationPolicy
    from execution_plane_transport_direct.pinned import PinnedTransport

    resolutions = []
    requests = []

    async def resolve(host, port) -> list[str]:
        resolutions.append(host)
        return ["192.0.2.10"] if len(resolutions) == 1 else ["127.0.0.1"]

    def respond(request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(202, json={"received": True})

    policy = DestinationPolicy(security_case.peers, resolver=resolve)
    transport = PinnedTransport(policy, "ep", httpx.MockTransport(respond))
    async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
        await client.post("https://ep.test/v1/commands", content=b"{}")
    assert len(resolutions) == 1
    assert requests[0].url.host == "192.0.2.10"
    assert requests[0].headers["host"] == "ep.test"
    assert requests[0].extensions["sni_hostname"] == "ep.test"
