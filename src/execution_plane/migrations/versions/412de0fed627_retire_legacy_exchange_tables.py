"""Remove tables made obsolete by command and Journal ownership."""

from collections.abc import Sequence

from alembic import op
from sqlalchemy import inspect

revision: str = "412de0fed627"
down_revision: str | Sequence[str] | None = "i5e6f7a8b9c0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OBSOLETE_TABLES = (
    "completion_events",
    "cluster_bindings",
    "command_records",
    "resource_revisions",
    "command_cancellation_intents",
)


def upgrade() -> None:
    """Drop obsolete local copies without migrating their rows elsewhere."""
    connection = op.get_bind()
    inspector = inspect(connection)
    for table_name in _OBSOLETE_TABLES:
        if inspector.has_table(table_name, schema="execution_plane"):
            op.drop_table(table_name, schema="execution_plane")


def downgrade() -> None:
    """Do not recreate removed persistence models on downgrade."""
