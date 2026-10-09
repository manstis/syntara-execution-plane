"""Real HTTPS handshake plus request assertion through the production EP runtime."""

import asyncio
import importlib.util
import json
import socket
import ssl
from pathlib import Path

import httpx
import pytest
import uvicorn
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from execution_plane.api.main import create_app
from execution_plane.api.tls import TLSH11Protocol
from execution_plane.config import EPSettings
from execution_plane_client.configuration import load_peer_trust
from execution_plane_client.protocol import MessageEnvelope
from execution_plane_client.security import AssertionSigner
from execution_plane_transport_journal.ep_runtime import build_ep_journal_runtime

from .command_helpers import command_database


async def test_real_mtls_requires_certificate_and_exact_signed_message(tmp_path, monkeypatch, work_message):  # noqa: PLR0915 - Full real TLS/HTTP/database boundary.
    generator_path = Path(__file__).resolve().parents[3] / "tools/generate_certs.py"
    spec = importlib.util.spec_from_file_location("ep_test_cert_generator", generator_path)
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    monkeypatch.setattr(generator, "CERTS_DIR", tmp_path)
    ca_key, ca_cert = generator.generate_ca()
    for name in ("ep", "ao"):
        generator.generate_service_cert(ca_key, ca_cert, name, name + ".service", ["localhost"])
    trust_entries = []
    signing_keys = {}
    with socket.socket() as server_socket:
        server_socket.bind(("127.0.0.1", 0))
        port = server_socket.getsockname()[1]
        for name in ("ep", "ao"):
            key = ec.generate_private_key(ec.SECP256R1())
            private = key.private_bytes(
                serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
            ).decode()
            public = (
                key.public_key()
                .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
                .decode()
            )
            (tmp_path / f"{name}.pem").write_text(private)
            signing_keys[name] = private
            base = f"https://localhost:{port if name == 'ep' else port + 1}/v1"
            trust_entries.append(
                {
                    "peer_id": name,
                    "role": "executor" if name == "ep" else "controller",
                    "issuer": "urn:" + name,
                    "audience": "urn:" + name,
                    "command_url": base + "/commands",
                    "query_base_url": base,
                    "grants": {
                        "ao": {
                            "project_ids": None,
                            "permissions": ["work.execute", "commands:read", "capabilities:read"],
                        }
                    },
                    "verification_keys": {"primary": public},
                    "permitted_networks": ["127.0.0.1/32"],
                    "certificate_common_names": [name + ".service"],
                }
            )
        trust_path = tmp_path / "peers.json"
        trust_path.write_text(json.dumps({"peers": trust_entries}))
        async with command_database() as db:
            settings = EPSettings(
                EP_PARTICIPANT_ID="ep",
                EP_COMMAND_PEERS_PATH=str(trust_path),
                EP_COMMAND_SIGNING_KEY_PATH=str(tmp_path / "ep.pem"),
                EP_API_TLS_CLIENT_CA_PATH=str(tmp_path / "ca.pem"),
                EP_API_TLS_CERT_PATH=str(tmp_path / "ep.crt"),
                EP_API_TLS_KEY_PATH=str(tmp_path / "ep.key"),
            )
            runtime = build_ep_journal_runtime(settings, db.sessions)
            config = uvicorn.Config(
                create_app(runtime=runtime),
                host="127.0.0.1",
                port=port,
                http=TLSH11Protocol,
                ssl_certfile=str(tmp_path / "ep.crt"),
                ssl_keyfile=str(tmp_path / "ep.key"),
                ssl_ca_certs=str(tmp_path / "ca.pem"),
                ssl_cert_reqs=ssl.CERT_REQUIRED,
                log_level="error",
                timeout_graceful_shutdown=2,
            )
            server = uvicorn.Server(config)
            task = asyncio.create_task(server.serve(sockets=[server_socket]))
            try:
                async with asyncio.timeout(5):
                    while not server.started:  # noqa: ASYNC110 - Uvicorn exposes a startup flag, not an event.
                        await asyncio.sleep(0.01)
                url = f"https://localhost:{port}"
                context = ssl.create_default_context(cafile=str(tmp_path / "ca.pem"))
                async with httpx.AsyncClient(verify=context, trust_env=False) as client:
                    with pytest.raises(httpx.HTTPError):
                        await client.get(url + "/healthz/live")
                context.load_cert_chain(str(tmp_path / "ao.crt"), str(tmp_path / "ao.key"))
                trust = load_peer_trust(trust_path)
                signer = AssertionSigner(
                    local_id="ao", issuer="urn:ao", key_id="primary", private_key=signing_keys["ao"], peers=trust.peers
                )
                values = work_message.model_dump()
                values["command"].update(
                    target_url=trust.peers["ep"].command_url, callback_url=trust.peers["ao"].command_url
                )
                message = MessageEnvelope.model_validate(values)
                async with httpx.AsyncClient(verify=context, trust_env=False) as client:
                    assert (
                        await client.post(url + "/v1/commands", json=message.model_dump(mode="json"))
                    ).status_code == 401
                    response = await client.post(
                        url + "/v1/commands",
                        json=message.model_dump(mode="json"),
                        headers={"Authorization": "Bearer " + signer.sign(message)},
                    )
                    assert response.status_code == 202, response.text
                    assert response.json()["message_id"] == str(message.message_id)
                    capabilities = await client.get(
                        url + "/v1/capabilities",
                        headers={
                            "Authorization": "Bearer " + signer.sign_query("ep", message.scope, "/v1/capabilities")
                        },
                    )
                    assert capabilities.status_code == 200
                    assert "cluster.provision" in capabilities.json()["commands"]
            finally:
                server.should_exit = True
                await task
