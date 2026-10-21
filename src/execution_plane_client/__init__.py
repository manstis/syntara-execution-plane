"""Shared command contracts and participant-owned handler registration."""

from execution_plane_client.catalog import CommandCatalog, CommandDefinition
from execution_plane_client.commands.credentials import CredentialResolution, CredentialResolve
from execution_plane_client.handlers import CommandHandler, HandlingResult
from execution_plane_client.protocol import Command, CommandScope, MessageEnvelope, TransportReceipt
from execution_plane_client.registry import CommandRegistry

__all__ = [
    "Command",
    "CommandCatalog",
    "CommandDefinition",
    "CommandHandler",
    "CommandRegistry",
    "CommandScope",
    "CredentialResolution",
    "CredentialResolve",
    "HandlingResult",
    "MessageEnvelope",
    "TransportReceipt",
]
