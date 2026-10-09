"""Participant observability contract; only safe correlation fields cross it."""

from collections.abc import Mapping
from typing import Protocol

type MetricValue = str | int | float | bool


class ExchangeObserver(Protocol):
    """Receive structured protocol measurements without private context."""

    def record(self, event: str, fields: Mapping[str, MetricValue]) -> None:
        """Record a safe event or metric at a protocol boundary."""
        ...


class NullObserver(ExchangeObserver):
    """Default observer without application logger or telemetry dependencies."""

    def record(self, event: str, fields: Mapping[str, MetricValue]) -> None:
        """Discard a protocol measurement."""
