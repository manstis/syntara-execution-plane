"""Retained empty revision after folding Journal fields into its creation baseline."""

from collections.abc import Sequence

revision: str = "ab600379be3e"
down_revision: str | Sequence[str] | None = "412de0fed627"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Use the optional Journal schema created from package metadata."""


def downgrade() -> None:
    """No separate context alteration remains in the greenfield schema."""
