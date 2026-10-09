"""Create the Journal transport's single durable exchange table."""

from __future__ import annotations

from typing import TYPE_CHECKING

from alembic import op

from execution_plane_transport_journal.schema import build_journal_schema

if TYPE_CHECKING:
    from collections.abc import Sequence

revision: str = "0001_exchange_journal"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create Journal-owned tables and indexes from the package schema."""
    build_journal_schema("execution_plane").metadata.create_all(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    """Remove the Journal-owned table and indexes."""
    build_journal_schema("execution_plane").metadata.drop_all(bind=op.get_bind(), checkfirst=True)
