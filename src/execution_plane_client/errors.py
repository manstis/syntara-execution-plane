"""Safe exception categories; never attach private envelopes or peer bodies."""

# ruff: noqa: N818 - Public protocol exception names are shared by participants.


class ProtocolError(Exception):
    """Base class for a safe, caller-visible command exchange error."""


class ProtocolConflict(ProtocolError):
    """An immutable identity was reused with different content."""


class UnsupportedCommand(ProtocolError):
    """A required command revision or handler is unavailable."""


class UnauthorizedPeer(ProtocolError):
    """Peer authentication or scoped authorization failed."""


class UnsafeDestination(ProtocolError):
    """A destination falls outside registered peer routing policy."""


class RetryableDelivery(ProtocolError):
    """Transport did not establish a definitive remote outcome."""


class PermanentRejection(ProtocolError):
    """An authorized peer definitively rejected the command."""


class StaleClaim(ProtocolError):
    """A worker no longer owns the current claim generation."""


class OutcomeUnknown(ProtocolError):
    """An external action requires observation before any further action."""


class CommandNotFound(ProtocolError):
    """No command exists within the caller's authorized scope."""


class InvalidCiphertext(ProtocolError):
    """Encrypted content could not be authenticated with the participant's keys."""


class HandlingDeferred(ProtocolError):
    """A handler conclusively performed no external effect and may safely retry."""


class CommandExecutionError(ProtocolError):
    """An expected domain failure identified by a participant-owned safe code."""

    def __init__(self, code: str) -> None:
        """Carry a static code, never a provider body, credential or task token."""
        super().__init__(code)
        self.code = code
