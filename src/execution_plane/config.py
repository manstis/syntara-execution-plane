"""Execution Plane configuration — reads from EP-owned environment variables."""

from __future__ import annotations

import base64
import ipaddress
from functools import lru_cache
from pathlib import Path

from pydantic import Field, computed_field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url

from execution_plane_client.configuration import ExchangeOptions

_AES_256_KEY_BYTES = 32


def to_asyncpg_url(database_url: str) -> str:
    """Return a PostgreSQL URL without a SQLAlchemy DBAPI driver suffix."""
    return make_url(database_url).set(drivername="postgresql").render_as_string(hide_password=False)


class EPSettings(BaseSettings):
    """Settings for the standalone Execution Plane service and worker."""

    model_config = SettingsConfigDict(extra="ignore")

    # Runtime configuration accepts only the EP-owned database variable. Alembic
    # has a separate DATABASE_URL input for the one-off migration job.
    database_url: str = Field(validation_alias="EP_DATABASE_URL")

    participant_id: str = Field(default="execution-plane", validation_alias="EP_PARTICIPANT_ID")
    command_peers_path: str | None = Field(default=None, validation_alias="EP_COMMAND_PEERS_PATH")
    command_signing_key_path: str | None = Field(default=None, validation_alias="EP_COMMAND_SIGNING_KEY_PATH")
    command_signing_key_id: str = Field(default="primary", validation_alias="EP_COMMAND_SIGNING_KEY_ID")
    command_tls_ca_path: str | None = Field(default=None, validation_alias="EP_COMMAND_TLS_CA_PATH")
    command_tls_cert_path: str | None = Field(default=None, validation_alias="EP_COMMAND_TLS_CERT_PATH")
    command_tls_key_path: str | None = Field(default=None, validation_alias="EP_COMMAND_TLS_KEY_PATH")
    command_options: ExchangeOptions = Field(default_factory=ExchangeOptions, validation_alias="EP_COMMAND_OPTIONS")
    journal_encryption_key_id: str = Field(default="primary", validation_alias="EP_JOURNAL_ENCRYPTION_KEY_ID")
    journal_decryption_keys: dict[str, str] = Field(
        default_factory=dict, validation_alias="EP_JOURNAL_DECRYPTION_KEYS", repr=False
    )

    node_startup_seconds: int = Field(default=120, validation_alias="EP_NODE_STARTUP_SECONDS", ge=1)
    node_grace_seconds: int = Field(default=30, validation_alias="EP_NODE_GRACE_SECONDS", ge=1)

    api_host: str = Field(default="127.0.0.1", validation_alias="EP_API_HOST")
    api_port: int = Field(default=8001, validation_alias="EP_API_PORT")
    api_tls_cert_path: str | None = Field(default=None, validation_alias="EP_API_TLS_CERT_PATH")
    api_tls_key_path: str | None = Field(default=None, validation_alias="EP_API_TLS_KEY_PATH")
    api_tls_client_ca_path: str | None = Field(default=None, validation_alias="EP_API_TLS_CLIENT_CA_PATH")
    credential_encryption_key: str | None = Field(default=None, validation_alias="EP_CREDENTIAL_ENCRYPTION_KEY")
    credential_encryption_key_path: str | None = Field(
        default=None,
        validation_alias="EP_CREDENTIAL_ENCRYPTION_KEY_PATH",
    )
    dispatch_retry_backoff_seconds: float = Field(default=5.0, validation_alias="EP_DISPATCH_RETRY_BACKOFF_SECONDS")
    workload_allowed_egress_cidrs: list[str] = Field(
        default_factory=list,
        validation_alias="EP_WORKLOAD_ALLOWED_EGRESS_CIDRS",
    )
    workload_forbidden_egress_cidrs: list[str] = Field(
        default_factory=list,
        validation_alias="EP_WORKLOAD_FORBIDDEN_EGRESS_CIDRS",
    )

    @model_validator(mode="after")
    def validate_workload_network_ranges(self) -> EPSettings:
        """Require explicit denies when a broad workload egress range is allowed."""
        allowed_networks = [ipaddress.ip_network(value, strict=False) for value in self.workload_allowed_egress_cidrs]
        forbidden_networks = [
            ipaddress.ip_network(value, strict=False) for value in self.workload_forbidden_egress_cidrs
        ]
        if allowed_networks and not forbidden_networks:
            msg = "EP_WORKLOAD_FORBIDDEN_EGRESS_CIDRS is required when workload egress is allowed"
            raise ValueError(msg)
        for network in allowed_networks:
            for denied in forbidden_networks:
                if isinstance(network, ipaddress.IPv4Network):
                    if not isinstance(denied, ipaddress.IPv4Network):
                        continue
                    partial_overlap = network.overlaps(denied) and not denied.subnet_of(network)
                else:
                    if not isinstance(denied, ipaddress.IPv6Network):
                        continue
                    partial_overlap = network.overlaps(denied) and not denied.subnet_of(network)
                if partial_overlap:
                    msg = f"Forbidden egress range {denied} must be contained by allowed range {network}"
                    raise ValueError(msg)
        return self

    @model_validator(mode="after")
    def load_credential_encryption_key(self) -> EPSettings:
        """Load EP's data key from a secret file and validate its AES-256 size."""
        if self.credential_encryption_key is None and self.credential_encryption_key_path:
            self.credential_encryption_key = (
                Path(self.credential_encryption_key_path).read_text(encoding="ascii").strip()
            )
        if self.credential_encryption_key is None:
            msg = "EP_CREDENTIAL_ENCRYPTION_KEY or EP_CREDENTIAL_ENCRYPTION_KEY_PATH must be configured"
            raise ValueError(msg)
        decoded_key = base64.urlsafe_b64decode(self.credential_encryption_key)
        if len(decoded_key) != _AES_256_KEY_BYTES:
            msg = "The Execution Plane credential encryption key must decode to 32 bytes"
            raise ValueError(msg)
        return self

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url_asyncpg(self) -> str:
        """asyncpg-compatible URL (strips the +asyncpg SQLAlchemy driver prefix)."""
        return to_asyncpg_url(self.database_url)


@lru_cache
def get_ep_settings() -> EPSettings:
    """Load and cache execution-plane settings from the environment."""
    # BaseSettings loads the required database_url from the environment.
    return EPSettings()  # type: ignore[call-arg]
