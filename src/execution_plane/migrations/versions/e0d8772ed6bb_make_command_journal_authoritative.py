"""Retained empty revision after making the Journal the only exchange record."""

from collections.abc import Sequence

revision: str = "e0d8772ed6bb"
down_revision: str | Sequence[str] | None = "ab600379be3e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """No cross-table responsibility transfer is part of the greenfield design."""


def downgrade() -> None:
    """No retired command tables are recreated by downgrade."""
