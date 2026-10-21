"""Credential-reference resolution commands and responses."""

from __future__ import annotations

from typing import Annotated, ClassVar
from uuid import UUID  # noqa: TC003 - Pydantic evaluates fields.

from pydantic import Field, StrictStr

from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.protocol import WireModel


class CredentialResolve(CommandPayload):
    """Resolve an opaque credential reference into a short-lived value."""

    command_type: ClassVar[str] = "credential.resolve"
    credential_ref: UUID


class CredentialResolution(WireModel):
    """The resolved credential value for one submitted command."""

    message_id: UUID
    credential: Annotated[StrictStr, Field(min_length=1, max_length=65536, repr=False)]
