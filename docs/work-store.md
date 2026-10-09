# Execution Plane: Work Store

`WorkStore` owns the lifecycle of EP's `WorkItem` records. The `work.execute`
command handler creates work in the same transaction as command handling.
Consumers use the command API; they never access EP's database or stores.

The worker claims pending items and records provider allocation, claim ownership,
execution state, results, and resource cleanup. `EncryptedWorkItemPayload`
protects the provider invocation persisted on the work item. It is domain
storage, not a delivery queue.

| Method | Responsibility |
| --- | --- |
| `dispatch()` | Persist work linked to its accepted command |
| `claim_one()` | Claim eligible work and quarantine expired dispatched attempts |
| `mark_dispatched()` | Record the boundary before external execution may start |
| `set_result()` | Persist a terminal outcome and its completion command atomically |
| `request_cancel()` | Cancel unstarted work or request cancellation of dispatched work |
| `mark_reconciliation_required()` | Record an uncertain outcome without re-executing it |
| `requeue_pre_execute_failure()` | Retry only when execution provably did not start |
| `is_target_drained()` | Check whether owned or uncertain work still uses a target |

Every outcome-producing transition calls the configured `EPCompletionRecorder`
inside the work transaction. The recorder appends `work.execute.completed` to the
shared journal and updates the command projection. Failure to journal rolls back
the work transition. An unconfigured recorder is an error.

Delivery, acceptance, retries and peer recovery belong to `execution_plane_client`.
There is no completion-event table, delivery retry method or cancellation
submission tombstone in `WorkStore`; command cancellation intents handle
cancellation arriving before submission. See [integration.md](integration.md)
for the handshake and [worker-manager.md](worker-manager.md) for provider recovery.
