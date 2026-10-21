"""Kubernetes API client factory from resolved Cluster credentials.

The vanilla Kubernetes Worker Manager can import this module when it needs the
same Cluster.endpoint construction. Do not log credentials.
"""

from __future__ import annotations

import tempfile
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from kubernetes import client

if TYPE_CHECKING:
    from collections.abc import Iterator

    from execution_plane.models.cluster import Cluster


class KubernetesClientError(ValueError):
    """The Cluster credential could not be turned into a Kubernetes client."""


def api_client_from_cluster(cluster: Cluster, credential: str) -> AbstractContextManager[client.ApiClient]:
    """Build an ApiClient from a Cluster row and resolved credential."""
    return api_client_from_credentials(
        endpoint=cluster.endpoint,
        api_key=credential,
        ca_certificate=cluster.ca_certificate,
    )


@contextmanager
def api_client_from_credentials(
    *,
    endpoint: str,
    api_key: str,
    ca_certificate: str | None = None,
) -> Iterator[client.ApiClient]:
    """Yield an ApiClient for a Cluster Bearer token.

    Cluster.endpoint is the control-plane URL. TLS uses the system CA bundle
    unless the Cluster stores a CA PEM. Never skip TLS verification by default.
    """
    if not endpoint.startswith(("http://", "https://")):
        message = "Cluster endpoint is not an HTTP Kubernetes API"
        raise KubernetesClientError(message)
    with tempfile.TemporaryDirectory(prefix="ep-kube-") as directory:
        configuration = _configuration_from_credentials(
            endpoint=endpoint.rstrip("/"),
            api_key=api_key,
            ca_certificate=ca_certificate,
            directory=Path(directory),
        )
        with client.ApiClient(configuration) as api_client:
            yield api_client


def read_namespace(cluster: Cluster, credential: str, namespace: str, *, timeout_seconds: float) -> None:
    """GET /api/v1/namespaces/{namespace}. Raises ApiException on non-2xx."""
    with api_client_from_cluster(cluster, credential) as api_client:
        client.CoreV1Api(api_client).read_namespace(namespace, _request_timeout=timeout_seconds)


def _configuration_from_credentials(
    *,
    endpoint: str,
    api_key: str,
    ca_certificate: str | None,
    directory: Path,
) -> client.Configuration:
    """Construct a client Configuration without logging or returning the credential."""
    configuration = client.Configuration()
    configuration.host = endpoint
    configuration.verify_ssl = True
    _apply_bearer_token(configuration, api_key)
    if ca_certificate:
        ca_path = directory / "ca.pem"
        ca_path.write_text(ca_certificate, encoding="utf-8")
        configuration.ssl_ca_cert = str(ca_path)
    return configuration


def _apply_bearer_token(configuration: client.Configuration, token: str) -> None:
    """Set both kubernetes-client auth keys so Bearer is not dropped (python#2595)."""
    configuration.api_key["authorization"] = token
    configuration.api_key["BearerToken"] = token
    configuration.api_key_prefix["authorization"] = "Bearer"
    configuration.api_key_prefix["BearerToken"] = "Bearer"
