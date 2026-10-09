# Commit-triggered command dispatch

The user approved implementing the assessed removal of polling latency in both
directions. Each participant keeps one journal as the durable contract. Journal
writes issue transactional PostgreSQL notifications, delivered only at commit.
A shared listener wakes outbound delivery and inbound handling across processes.
No HTTP occurs before commit or while a domain transaction holds locks.

The listener uses a dedicated autocommit connection obtained from participant
sessions, includes a health probe, reconnects after failure, and rescans on startup
and reconnection. Notification payloads contain only a work category, never command
content or private context. Channels are scoped to the journal table/schema.
Notifications are hints: missed notifications and disconnected listeners fall
back to durable scans. No schema or protocol migration is needed.

Both delivery and handling use a bounded, continuously replenished set of tasks.
Available capacity accepts new committed work without waiting for a whole batch
or a fixed sleep. Workers clear wake events before scanning to avoid losing a
commit racing the scan. Retry, acceptance, observation and expired-lease deadlines
remain journal-owned; the scheduler waits until the earlier of the next deadline,
a notification, task completion, or the configured fallback scan interval.

All command kinds use the same path, including reverse completions and acceptance
commands. Rearmed local handling and reopened acceptance delivery also notify.
AO APIs may enqueue in a different process from the Temporal worker, so in-process
signals alone cannot implement the requirement. Existing provider code remains
unchanged. Producer calls continue returning after durable commit; they do not
wait for a peer HTTP response. Immediate dispatch means no intentional polling
wait when worker capacity is available, not a hard real-time delivery guarantee.

Work in the original checkouts, without worktrees. Commit locally with Codex
attribution and do not push or publish. Refresh AO's pinned client archive after
the EP implementation commit. Test with real PostgreSQL and an installed peer.
