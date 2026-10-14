"""Safe protocol errors, without request bodies or infrastructure details."""

# ruff: noqa: N818


class ProtocolConflict(Exception):
    """Duplicate registration or conflicting resource state."""


class UnsupportedCommand(Exception):
    """The command type or revision is unsupported by this participant."""
