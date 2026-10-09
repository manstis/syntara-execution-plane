"""Versioned journal metadata, isolated from application ORM registries."""

from dataclasses import dataclass

from sqlalchemy import (
    UUID,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
)

SCHEMA_VERSION = 3


@dataclass(frozen=True)
class JournalSchema:
    """Metadata a participant explicitly includes in its own migration chain."""

    metadata: MetaData
    table: Table
    version: int


def build_journal_schema(schema: str | None = None) -> JournalSchema:
    """Build a fresh table definition without mutating SQLModel.metadata."""
    metadata = MetaData()
    table = Table(
        "exchange_journal",
        metadata,
        Column("entry_id", UUID(as_uuid=True), primary_key=True),
        Column("message_id", UUID(as_uuid=True), nullable=False),
        Column("command_id", UUID(as_uuid=True), nullable=False),
        Column("parent_command_id", UUID(as_uuid=True)),
        Column("parent_sequence", BigInteger),
        Column("message_type", String(64), nullable=False),
        Column("command_type", String(128), nullable=False),
        Column("command_revision", Integer, nullable=False),
        Column("sequence", BigInteger, nullable=False),
        Column("source_id", String(255), nullable=False),
        Column("destination_id", String(255), nullable=False),
        Column("client_id", String(255), nullable=False),
        Column("project_id", UUID(as_uuid=True)),
        Column("all_projects", Boolean, nullable=False),
        Column("direction", String(16), nullable=False),
        Column("target_url", Text, nullable=False),
        Column("callback_url", Text, nullable=False),
        Column("envelope_hash", String(64), nullable=False),
        Column("envelope_ciphertext", Text, nullable=False),
        Column("delivery_state", String(32), nullable=False),
        Column("handling_state", String(32), nullable=False),
        Column("command_status", String(32), nullable=False, server_default="accepted"),
        Column("resource_type", String(64)),
        Column("resource_key", String(255)),
        Column("resource_id", UUID(as_uuid=True)),
        Column("desired_revision", BigInteger),
        Column("desired_hash", String(64)),
        Column("observed_revision", BigInteger, nullable=False, server_default="0"),
        Column("resource_head", Boolean, nullable=False, server_default="false"),
        Column("resource_deleted", Boolean, nullable=False, server_default="false"),
        Column("target_command_id", UUID(as_uuid=True)),
        Column("command_error_code", String(128)),
        Column("observation_pending", Boolean, nullable=False, server_default="false"),
        Column("delivery_attempts", Integer, nullable=False, server_default="0"),
        Column("delivery_due_at", DateTime(timezone=True)),
        Column("delivery_owner", UUID(as_uuid=True)),
        Column("delivery_generation", BigInteger, nullable=False, server_default="0"),
        Column("delivery_lease_until", DateTime(timezone=True)),
        Column("acceptance_due_at", DateTime(timezone=True)),
        Column("handling_attempts", Integer, nullable=False, server_default="0"),
        Column("handling_due_at", DateTime(timezone=True)),
        Column("handling_owner", UUID(as_uuid=True)),
        Column("handling_generation", BigInteger, nullable=False, server_default="0"),
        Column("handling_lease_until", DateTime(timezone=True)),
        Column("handling_error_code", String(128)),
        Column("delivery_error_code", String(128)),
        Column("reconciliation_required", Boolean, nullable=False, server_default="false"),
        Column("reconciliation_due_at", DateTime(timezone=True)),
        Column("reconciliation_error_code", String(128)),
        Column("prepared_effect_ciphertext", Text),
        Column("local_context_ciphertext", Text),
        Column("local_context_generation", BigInteger, nullable=False, server_default="0"),
        Column("correlation_key", String(255)),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("received_at", DateTime(timezone=True)),
        Column("sent_at", DateTime(timezone=True)),
        Column("accepted_at", DateTime(timezone=True)),
        Column("handled_at", DateTime(timezone=True)),
        Column("updated_at", DateTime(timezone=True), nullable=False),
        UniqueConstraint("message_id", "direction", name="uq_exchange_message_direction"),
        UniqueConstraint("client_id", "command_id", "direction", name="uq_exchange_client_command_direction"),
        UniqueConstraint(
            "client_id", "parent_command_id", "parent_sequence", "direction", name="uq_exchange_parent_sequence"
        ),
        CheckConstraint("direction IN ('inbound', 'outbound')", name="ck_exchange_direction"),
        CheckConstraint("command_revision > 0 AND sequence >= 0", name="ck_exchange_revision_sequence"),
        CheckConstraint(
            "(project_id IS NULL AND all_projects) OR (project_id IS NOT NULL AND NOT all_projects)",
            name="ck_exchange_scope",
        ),
        CheckConstraint(
            "parent_sequence IS NULL OR (parent_sequence > 0 AND parent_command_id IS NOT NULL)",
            name="ck_exchange_parent_sequence",
        ),
        CheckConstraint(
            "NOT resource_head OR (direction = 'inbound' AND resource_type IS NOT NULL "
            "AND resource_key IS NOT NULL AND desired_revision IS NOT NULL AND desired_hash IS NOT NULL)",
            name="ck_exchange_resource_head",
        ),
        CheckConstraint(
            "observed_revision >= 0 AND (desired_revision IS NULL OR desired_revision > 0)",
            name="ck_exchange_resource_revision",
        ),
        schema=schema,
    )
    Index("ix_exchange_delivery_due", table.c.direction, table.c.delivery_state, table.c.delivery_due_at)
    Index("ix_exchange_acceptance_due", table.c.delivery_state, table.c.acceptance_due_at)
    Index("ix_exchange_handling_due", table.c.handling_state, table.c.handling_due_at)
    Index("ix_exchange_reconciliation_due", table.c.reconciliation_required, table.c.reconciliation_due_at)
    Index("ix_exchange_scope", table.c.client_id, table.c.project_id, table.c.command_id)
    Index("ix_exchange_correlation", table.c.client_id, table.c.correlation_key, table.c.entry_id)
    Index("ix_exchange_children", table.c.client_id, table.c.parent_command_id, table.c.parent_sequence)
    Index("ix_exchange_cancel_target", table.c.client_id, table.c.target_command_id, table.c.direction)
    Index("ix_exchange_resource", table.c.client_id, table.c.resource_type, table.c.resource_id)
    Index(
        "uq_exchange_resource_head",
        table.c.client_id,
        table.c.resource_type,
        table.c.resource_key,
        unique=True,
        postgresql_where=table.c.resource_head,
    )
    Index(
        "uq_exchange_resource_owner",
        table.c.resource_type,
        table.c.resource_id,
        unique=True,
        postgresql_where=table.c.resource_head & table.c.resource_id.is_not(None),
    )
    return JournalSchema(metadata, table, SCHEMA_VERSION)
