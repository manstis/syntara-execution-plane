"""Execution-target configuration and independent registration commands."""

from __future__ import annotations

import re
from typing import Annotated, Literal, Self
from uuid import UUID  # noqa: TC003 - SQLModel evaluates fields.

from pydantic import Field, StrictBool, StrictStr, field_validator, model_validator

from execution_plane_client.commands import CommandIdentity, CommandRoute, ResourceRevision, build_command
from execution_plane_client.protocol import Command, MessageEnvelope, WireModel

_LABEL_NAME = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$")
_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_LABEL_LIMIT = 63
_DOMAIN_LIMIT = 253


def _label_key(value: str) -> bool:
    prefix, separator, name = value.partition("/")
    if separator:
        if (
            not prefix
            or len(prefix) > _DOMAIN_LIMIT
            or any(len(part) > _LABEL_LIMIT or not _DNS_LABEL.fullmatch(part) for part in prefix.split("."))
        ):
            return False
    else:
        name = prefix
    return len(name) <= _LABEL_LIMIT and bool(_LABEL_NAME.fullmatch(name))


def _label_value(value: str) -> bool:
    return len(value) <= _LABEL_LIMIT and (value == "" or bool(_LABEL_NAME.fullmatch(value)))


class KubernetesPlacement(WireModel):
    """Kubernetes placement contract, independent of Kubernetes libraries."""

    type: Literal["kubernetes"] = "kubernetes"
    namespace: Annotated[StrictStr, Field(min_length=1, max_length=63, pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")]
    node_selectors: list[str] = Field(default_factory=list)
    tolerations: list[str] = Field(default_factory=list)

    @field_validator("node_selectors")
    @classmethod
    def check_selectors(cls, values: list[str]) -> list[str]:
        """Accept only Kubernetes key=value selectors."""
        for value in values:
            key, separator, label = value.partition("=")
            if not separator or not _label_key(key) or not _label_value(label):
                message = "Invalid Kubernetes node selector"
                raise ValueError(message)
        return values

    @field_validator("tolerations")
    @classmethod
    def check_tolerations(cls, values: list[str]) -> list[str]:
        """Accept compact tolerations with supported scheduling effects."""
        for value in values:
            key_value, separator, effect = value.rpartition(":")
            if not separator:
                key_value, effect = value, ""
            key, equals, label = key_value.partition("=")
            valid_key = bool(key) and _label_key(key) if equals else not key or _label_key(key)
            if (
                not valid_key
                or not _label_value(label)
                or effect not in {"", "NoSchedule", "PreferNoSchedule", "NoExecute"}
            ):
                message = "Invalid Kubernetes toleration"
                raise ValueError(message)
        return values


class RHELPlacement(WireModel):
    """Placement for a supported RHEL backend."""

    type: Literal["rhel"] = "rhel"


Placement = Annotated[KubernetesPlacement | RHELPlacement, Field(discriminator="type")]


class TargetDefinition(WireModel):
    """Desired execution-target configuration."""

    name: Annotated[StrictStr, Field(min_length=1, max_length=255)]
    backend_type: Literal["vanilla_k8s", "openshell"]
    placement: Placement
    endpoint: Annotated[StrictStr, Field(min_length=1, max_length=2048)]
    credential: str = Field(default="", repr=False)
    labels: dict[str, str] = Field(default_factory=dict)

    @field_validator("endpoint")
    @classmethod
    def check_endpoint(cls, value: str) -> str:
        """Preserve verified HTTPS for provider management credentials."""
        return Command.check_url(value)

    @model_validator(mode="after")
    def check_backend(self) -> Self:
        """Reject backend/placement pairs unsupported by the provider adapters."""
        if (self.backend_type == "vanilla_k8s") != isinstance(self.placement, KubernetesPlacement):
            message = "Backend does not support this placement"
            raise ValueError(message)
        return self


class TargetRegister(TargetDefinition, ResourceRevision):
    """Register an independently identified target on an existing cluster."""

    cluster_id: UUID
    is_default: StrictBool = False


class TargetUpdate(ResourceRevision):
    """Mutable target fields; cluster and default identity cannot be reassigned."""

    name: Annotated[StrictStr, Field(min_length=1, max_length=255)] | None = None
    endpoint: Annotated[StrictStr, Field(min_length=1, max_length=2048)] | None = None
    placement: Placement | None = None
    credential: str | None = Field(default=None, repr=False)
    labels: dict[str, str] | None = None

    @field_validator("endpoint")
    @classmethod
    def check_endpoint(cls, value: str | None) -> str | None:
        """Validate explicit endpoint updates before durable acceptance."""
        return None if value is None else Command.check_url(value)


def target_register(*, identity: CommandIdentity, route: CommandRoute, payload: TargetRegister) -> MessageEnvelope:
    """Build an independent target registration."""
    return build_command("execution_target.register", identity=identity, route=route, payload=payload)


def target_update(*, identity: CommandIdentity, route: CommandRoute, payload: TargetUpdate) -> MessageEnvelope:
    """Build a revisioned target update, retaining omitted-versus-null fields."""
    return build_command("execution_target.update", identity=identity, route=route, payload=payload)


def target_delete(*, identity: CommandIdentity, route: CommandRoute, payload: ResourceRevision) -> MessageEnvelope:
    """Build a revisioned target drain/delete request."""
    return build_command("execution_target.delete", identity=identity, route=route, payload=payload)
