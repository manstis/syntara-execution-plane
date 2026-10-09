"""Independent cluster registration, provisioning and revisioned changes."""

import ssl
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, StrictBool, StrictStr, field_validator

from execution_plane_client.commands import CommandIdentity, CommandRoute, ResourceRevision, build_command
from execution_plane_client.commands.targets import TargetDefinition
from execution_plane_client.protocol import Command, MessageEnvelope


def _validate_ca(certificate: str | None) -> str | None:
    if certificate is not None:
        try:
            ssl.create_default_context().load_verify_locations(cadata=certificate)
        except ssl.SSLError as exc:
            message = "CA certificate must contain valid PEM"
            raise ValueError(message) from exc
    return certificate


class ClusterRegister(ResourceRevision):
    """Record cluster configuration without creating an execution target."""

    name: Annotated[StrictStr, Field(min_length=1, max_length=255)]
    endpoint: Annotated[StrictStr, Field(min_length=1, max_length=2048)]
    credential: str = Field(repr=False)
    ca_certificate: Annotated[str, Field(max_length=65536)] | None = Field(default=None, repr=False)
    cluster_type: Literal["openshift", "rhel"] = "openshift"
    project_ids: list[UUID] | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    enabled: StrictBool = True

    @field_validator("endpoint")
    @classmethod
    def check_endpoint(cls, value: str | None) -> str | None:
        """Keep management credentials on explicit HTTPS endpoints."""
        return None if value is None else Command.check_url(value)

    @field_validator("ca_certificate")
    @classmethod
    def check_ca(cls, value: str | None) -> str | None:
        """Validate the trust bundle without contacting its destination."""
        return _validate_ca(value)


class ClusterProvision(ClusterRegister):
    """Create or converge a cluster and its default execution target."""

    default_target: TargetDefinition


class ClusterUpdate(ResourceRevision):
    """Optional mutable cluster fields; explicit null can clear optional values."""

    name: Annotated[StrictStr, Field(min_length=1, max_length=255)] | None = None
    endpoint: Annotated[StrictStr, Field(min_length=1, max_length=2048)] | None = None
    credential: str | None = Field(default=None, repr=False)
    ca_certificate: Annotated[str, Field(max_length=65536)] | None = Field(default=None, repr=False)
    project_ids: list[UUID] | None = None
    labels: dict[str, str] | None = None
    enabled: StrictBool | None = None

    @field_validator("endpoint")
    @classmethod
    def check_endpoint(cls, value: str | None) -> str | None:
        """Keep management credentials on explicit HTTPS endpoints."""
        return None if value is None else Command.check_url(value)

    @field_validator("ca_certificate")
    @classmethod
    def check_ca(cls, value: str | None) -> str | None:
        """Validate explicit trust-bundle updates."""
        return _validate_ca(value)


def cluster_register(*, identity: CommandIdentity, route: CommandRoute, payload: ClusterRegister) -> MessageEnvelope:
    """Build cluster-only registration."""
    return build_command("cluster.register", identity=identity, route=route, payload=payload)


def cluster_provision(*, identity: CommandIdentity, route: CommandRoute, payload: ClusterProvision) -> MessageEnvelope:
    """Build revisioned cluster/default-target convergence."""
    return build_command("cluster.provision", identity=identity, route=route, payload=payload)


def cluster_update(*, identity: CommandIdentity, route: CommandRoute, payload: ClusterUpdate) -> MessageEnvelope:
    """Build a cluster update retaining omitted-versus-null fields."""
    return build_command("cluster.update", identity=identity, route=route, payload=payload)


def cluster_delete(*, identity: CommandIdentity, route: CommandRoute, payload: ResourceRevision) -> MessageEnvelope:
    """Build a cluster drain/delete command."""
    return build_command("cluster.delete", identity=identity, route=route, payload=payload)
