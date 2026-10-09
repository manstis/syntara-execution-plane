# Execution Plane: Work Executor

Work enters EP as a `work.execute` command through `POST /v1/commands`.
`execution_plane_client` supplies the envelope, typed payload, authentication,
ingress, encrypted journal, delivery, handling and recovery workers.

The caller allocates the command identity and journals the immutable request in
its own domain transaction. EP durably receives and acknowledges it before
asynchronous handling. Duplicate identities confirm the same frozen command;
conflicting contents are rejected.

EP's `WorkCommandHandler` creates a `WorkItem` through `WorkStore`, records its
resource reference and observes cancellation intents. The work item has its own
EP-assigned identifier; the caller's command ID remains the stable request key.
The provider worker then claims and executes eligible work.

Provider outcomes commit with child completion commands in the EP journal.
The shared workers deliver those commands to the original callback destination.
AO handles the received result using its local Temporal correlation. Queries
provide scoped command, child and resource observations for recovery.

See [integration.md](integration.md) for the service boundary and
[work-store.md](work-store.md) for atomic provider outcome recording.
