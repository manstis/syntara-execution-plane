"""Transport classification retains ambiguity and never saves peer secrets."""

import httpx
import pytest


@pytest.mark.parametrize(
    ("status", "exception"),
    [
        (429, "RetryableDelivery"),
        (500, "RetryableDelivery"),
        (422, "PermanentRejection"),
        (409, "PermanentRejection"),
        (307, "UnsafeDestination"),
    ],
)
async def test_delivery_errors_are_safe(security_case, message, status, exception):
    from execution_plane_client import errors
    from execution_plane_client.transport import HttpCommandTransport

    def respond(request) -> httpx.Response:
        return httpx.Response(
            status, text="private-peer-result-and-token", headers={"location": "https://unregistered.test"}
        )

    transport = HttpCommandTransport(
        signer=security_case.signer, peers=security_case.peers, transports={"ep": httpx.MockTransport(respond)}
    )
    try:
        with pytest.raises(getattr(errors, exception)) as failure:
            await transport.send(message)
        assert "private-peer" not in str(failure.value)
    finally:
        await transport.aclose()


@pytest.mark.parametrize("body", [b"{}", b'{"received":true,"received":false}', b"not json"])
async def test_malformed_success_is_uncertain(security_case, message, body):
    from execution_plane_client.errors import RetryableDelivery
    from execution_plane_client.transport import HttpCommandTransport

    transport = HttpCommandTransport(
        signer=security_case.signer,
        peers=security_case.peers,
        transports={"ep": httpx.MockTransport(lambda _request: httpx.Response(202, content=body))},
    )
    try:
        with pytest.raises(RetryableDelivery):
            await transport.send(message)
    finally:
        await transport.aclose()


async def test_transport_receipt_does_not_imply_handshake_acceptance(security_case, message):
    from execution_plane_client.transport import HttpCommandTransport

    requests = []

    def respond(request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(202, json={"received": True, "message_id": str(message.message_id)})

    transport = HttpCommandTransport(
        signer=security_case.signer, peers=security_case.peers, transports={"ep": httpx.MockTransport(respond)}
    )
    try:
        receipt = await transport.send(message)
        assert receipt.message_id == message.message_id
        assert receipt.received is True
        assert requests[0].headers["authorization"].startswith("Bearer ")
    finally:
        await transport.aclose()


async def test_observer_only_receives_safe_response_measurements(security_case, message):
    from execution_plane_client.errors import RetryableDelivery
    from execution_plane_client.transport import HttpCommandTransport

    events = []

    class Observer:
        def record(self, event, fields) -> None:
            events.append((event, fields))

    transport = HttpCommandTransport(
        signer=security_case.signer,
        peers=security_case.peers,
        observer=Observer(),
        transports={"ep": httpx.MockTransport(lambda _request: httpx.Response(202, content=b"private-result"))},
    )
    try:
        with pytest.raises(RetryableDelivery):
            await transport.send(message)
        assert {event for event, _fields in events} == {"transport.response", "transport.retry", "transport.latency"}
        assert "private-result" not in str(events)
        assert "authorization" not in str(events).lower()
        assert "payload" not in str(events)
    finally:
        await transport.aclose()


async def test_resource_reads_bind_assertion_to_bounded_query_path(security_case, message):
    from execution_plane_client.transport import HttpCommandTransport

    requests = []

    def respond(request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[])

    transport = HttpCommandTransport(
        signer=security_case.signer, peers=security_case.peers, transports={"ep": httpx.MockTransport(respond)}
    )
    try:
        assert await transport.get_resource("ep", message.scope, "execution-targets", limit=2) == []
        path = requests[0].url.raw_path.decode()
        assert path.endswith("/execution-targets?limit=2")
        identity = security_case.verifier.verify_query(
            requests[0].headers["authorization"].removeprefix("Bearer "), path
        )
        assert identity.scope == message.scope
        with pytest.raises(ValueError, match="page bounds"):
            await transport.get_resource("ep", message.scope, "clusters", limit=1000)
    finally:
        await transport.aclose()
