# One journal for command intent and status

## Direction

The user's 12 October correction supersedes the separate command projections in
the 8/9 October designs: each participant owns one `exchange_journal` table,
defined and managed by `execution_plane_client`, for the durable bilateral
contract. Remove EP's `CommandRecord`, `ResourceRevision`, and
`CommandCancellationIntent` tables and their store. `ResourceReference` remains
a nonpersistent wire value. Existing WorkItem, Cluster and ExecutionTarget
tables retain their provider/domain responsibilities.

The existing approved implementation is being corrected in the original EP and
AO checkouts. No worktrees, pushes or publication. Both applications continue to
depend on the separately installable client package; only EP is containerized
by the EP repository.

## Journal intent and observations

Add nullable indexed resource type/key, desired revision/hash, resource ID,
observed revision, cancellation target ID and command error fields. Add a
resource-head marker and deletion marker. These are metadata on actual command
rows, never synthetic resource rows or another table. Immutable envelopes remain
encrypted and unchanged. Private AO Temporal context remains encrypted.

For inbound resource actions, one journal row is the current reservation for
each client/resource type/logical key. A partial unique index enforces this.
Newer accepted revisions atomically move this marker to their own journal row,
carrying the last observed physical identity, revision and deletion state.
Historical rows retain their own command outcomes. Equal revision/different
intent conflicts; equal revision/same intent converges; older intent is recorded
but is superseded when handled. Scope must match exactly across a logical
resource. Head rows preserve deletion guards beyond physical resource lifetime.

For inbound commands, the shared journal takes transaction advisory locks before
receipt insertion or handling-family locks: a logical-resource lock for resource actions, and the
target command identity lock for cancellation (the original identity for work).
This protects absent rows and prevents admission/cancellation and resource-head
transfer races. Participant adapters never implement these durability rules.
Outbound enqueue and completion/control messages do not take these intent locks;
their family locks retain parent-first ordering.

An accepted cancellation journal entry indexes its target command. Exact scope
matching protects later work admission, including when cancellation is received
before the original. No separate cancellation lifecycle table remains.

## Shared local observation

Handlers may return a durable request for another observation after a delay.
The shared handling worker records this on the journal, claims it with its
existing independent lease/generation and invokes an explicit `observe` method.
An observation-pending flag survives an expired processing lease; a restart must
never convert observation into repeating the initial mutation. EP handlers
inspect drain/cancel progress; the shared layer owns scheduling, fencing, status,
child completion creation and retry. Remove EPResourceReconciler and its loop.

Queries report command state and resource references from journal entries only.
Resource GET endpoints may still expose provider state. A work command becomes
`running` when its domain handler admits it to provider execution, including time
queued for a worker; resource reads expose `pending`/`claimed`/`dispatched`.
Confirmed cancellation intent changes the original command to `cancel_requested`
in the same transaction as the WorkItem cancellation request. Work completion records
the bounded outgoing result, parent status and resource association atomically
with the WorkItem transition. Received completion updates the outbound parent's
same journal metadata, preserving child sequence ordering.

## Migration and verification

Both participants apply the same new journal schema through their Alembic chains.
EP removes the three obsolete tables through a forward migration. Existing
authoritative records must be transferred without losing revision tombstones,
cancellation targets, scope or pending observations; if a record cannot be
represented safely, abort with counts rather than delete it. No user database is
migrated during development. Test migrations against disposable PostgreSQL.

Regressions exercise actual PostgreSQL transactions: concurrent resource receipt
and handling, stale and duplicate revisions, cross-scope rejection, cancellation
before/during admission, observation restart/fencing, completion rollback and
duplicate suppression, authorized queries, and bilateral completion/recovery.
Update AO schema/tests and its client-source pin after the EP implementation commit.
