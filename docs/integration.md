# AO and EP command exchange

AO owns workflow authorization, Temporal activities, integrations and task
tokens. EP owns accepted work, clusters, execution targets, provider attempts
and results. Both depend on the independently installable
`execution-plane-client` package. AO never imports the EP service or accesses
its database. Each participant owns its journal schema and encryption keys.

## Durable submission and completion

AO appends a `work.execute` envelope and encrypted AO-private journal context in the same
transaction. Submission does not perform HTTP. A journal worker sends the
frozen envelope to EP's `POST /v1/commands` using mTLS and an ES256 assertion
bound to the complete message. EP atomically commits the inbound command and
its outbound `command.accepted` before returning HTTP 202. HTTP receipt means
the peer persisted the message; acceptance and execution are separate states.

EP commits each terminal work observation with a `work.execute.completed`
child command. Its `target_url` is the original callback URL and its
`callback_url` is EP's own command ingress. AO receives it through the same
journal protocol at `/api/execution_plane/v1/commands`, acknowledges it, then
bridges the result to Temporal. Acceptance controls never receive acceptance
controls, preventing acknowledgement loops.

A stable command UUID identifies one Temporal activity within one workflow
run and loop iteration. Temporal retries replace the encrypted task token
under a generation fence and reuse the original command and payload. Delivery,
handling and reconciliation have independent leases and retry schedules.
Prepared external effects survive restarts; an ambiguous provider or Temporal
RPC is observed or reported as `outcome_unknown`, never blindly repeated.
Safe pre-execution retries obey the command's optional attempt limit.

Cancellation is another durable command. An intent arriving before work blocks
its execution. Acceptance does not mean a running workload has stopped; EP
reports the observed outcome and preserves terminal-result races.

## Resource commands and reads

`cluster.register` and `execution_target.register` are independent operations.
`cluster.provision` creates or converges a cluster and its default target.
Updates and deletions carry a logical resource key and desired revision.
Older revisions become superseded; deletion tombstones prevent resurrection.
Disable/delete completes only after EP observes draining and cleanup.

AO freezes management credentials, project assignments and desired settings
with each integration revision. Only the current revision can change AO's UI
state. Disabling an integration can use its stored credential to drain even
if that credential is disabled for new work.

Commands are the only public mutation API. Scoped GET endpoints provide
command status, ordered child commands, capabilities, work items, clusters and
execution targets. Command status reads exclude private payloads. Resource
reads exclude invocation data and credentials. Work-item lists omit result
bodies and set `result_omitted`; individual reads include results up to 512 KiB,
or explicitly omit them with `result_size_bytes`. The complete result remains
in EP storage. Recovery of original completion envelopes is restricted to
their intended peer. Both client and project scope are enforced.

Completion envelopes reserve their entire serialized size within 512 KiB. An
oversized provider result remains durably stored with its actual WorkItem
status, while the command reports `failed` / `completion_result_too_large`
with a work-item reference (uncertain outcomes remain `outcome_unknown`). AO
fails the waiting activity without retrying execution. Oversized result bodies
require operator access to EP storage; this version has no result download API.
Private encrypted preparation records are validated as finite JSON without the
wire-size cap, so participant token and identity metadata cannot turn a valid
received completion into an undeliverable record.

## Deployment and cutover

Check out `syntara` and `syntara-execution-plane` side by side; see the
[README](../README.md) for installation, builds and peer configuration. EP
publishes separate direct and Journal service images; AO installs only the
client distribution. Both services need explicit peer routes,
verification keys, signing keys, trusted client certificates and journal
at-rest encryption keys. Key rotation retains previous decryption and
verification keys until their obligations have drained.

Migrations refuse unresolved legacy delivery and resource observations.
Completed legacy AO outcomes remain read-only history. There are no legacy
write APIs or legacy transport loops. Stop old writers before upgrading; do
not bypass preflight by discarding pending rows.

## Node communication and validation

The provider still uses the SDK gRPC protocol over an authenticated Kubernetes
API port-forward. EP never connects to Temporal. See
[cold-start-node-dispatch.md](cold-start-node-dispatch.md) for provider recovery.

The [validation record](superpowers/plans/2026-10-09-execution-plane-client-validation.md)
records actual checks and limitations. AO's bilateral tests run EP from its
non-editable image in a separate process, use independent journal schemas and
inject only the network and provider/Temporal boundaries. They kill and
restart EP at durable boundaries and verify provider calls are not repeated.
The real TLS tests and kind regression cover different boundaries; a skipped
kind test is not evidence of Kubernetes execution.
