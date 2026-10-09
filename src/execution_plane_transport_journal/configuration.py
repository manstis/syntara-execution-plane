"""Bounded worker timing for Journal-backed command exchange."""

from pydantic import Field

from execution_plane_client.protocol import WireModel


class ExchangeOptions(WireModel):
    """Durable worker timing independent of provider execution policy."""

    poll_seconds: float = Field(default=1, gt=0)
    reconciliation_seconds: float = Field(default=5, gt=0)
    lease_seconds: float = Field(default=30, gt=0)
    retry_base_seconds: float = Field(default=1, gt=0)
    retry_max_seconds: float = Field(default=60, gt=0)
    request_timeout_seconds: float = Field(default=10, gt=0)
    batch_size: int = Field(default=10, ge=1, le=100)
