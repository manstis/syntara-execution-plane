# Commit-triggered command dispatch implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox syntax for tracking.

**Goal:** Remove normal polling delays from AO→EP and EP→AO transport and handling.

**Architecture:** Transactional journal notifications wake a shared listener; bounded workers replenish free capacity and schedule retries from persisted deadlines. Polling remains recovery fallback.

**Tech Stack:** Python, asyncio, asyncpg, SQLAlchemy, PostgreSQL, pytest, uv.

**Spec:** `docs/superpowers/specs/2026-10-12-command-wakeups-design.md`

## Global constraints

- Original checkouts only; no worktrees, pushes, publication, or user database changes.
- One journal per participant; no added persistence table or wire API change.
- Keep provider code unchanged; execute HTTP only after committing claims.
- Every commit includes `Co-authored-by: OpenAI Codex <noreply@openai.com>`.

## Review focus

- Rollback and savepoint rollback must not publish wakeups for aborted work.
- A commit racing an empty scan must not lose its wakeup.
- A slow send/handler must not block free capacity; all tasks must stop on shutdown.
- Lost notifications, listener reconnect and startup must preserve recovery.
- Long fallback intervals must not delay retry, observation or acceptance deadlines.

## Task 1: Shared notifications and event-driven scheduling

**Files:** new `src/execution_plane_client/journal/notifications.py`,
`workers/scheduling.py`; update `journal/{store,claims}.py`,
`workers/{delivery,handling}.py`, `service.py`, client `pyproject.toml`, `uv.lock`.
Tests: new `tests/integration/postgres/test_exchange_wakeups.py`; extend
`exchange_helpers.py`. Scheduler behaviour is covered by the PostgreSQL tests.

**Interfaces:** `notify_journal(session, schema, kind)` issues transactional
`pg_notify`; `JournalNotifications.run()` drives delivery/handling events;
`JournalClaims.next_delivery_at(session)` and `next_handling_at(session)` return
persisted eligibility deadlines. `run_claimed(claim, process, next_due, wakeup,
clock, limit, poll_seconds)` supervises bounded work. Existing `run_once()` stays
available for deterministic recovery tests.

- [x] Write regression using `peer_pair(options=ExchangeOptions(poll_seconds=60))`:
  start both services; commit work after listeners/scans are idle; await real
  HTTP exchange and completion within 3 seconds, without calling `run_once()`.
- [x] Run the regression; expect timeout with the current polling implementation.
- [x] Implement journal signals on append/rearm/reopened acceptance/permanent
  rejection. Use `await session.execute(select(func.pg_notify(channel, kind)))`.
- [x] Implement a supervised listener with autocommit, committed LISTEN followed
  by rescan, bounded health checks, reconnect and cancellation cleanup.
- [x] Implement scheduling with `asyncio.TaskGroup`, free-capacity claims, task
  completion wakeups, and `min(poll_seconds, next_due - clock())` deadlines.
- [x] Test rollback, cross-connection wakeup, startup/reconnect/fallback recovery,
  slow task isolation, duplicate claim arbitration, due retries and shutdown.
- [x] Run focused PostgreSQL tests, Ruff and strict mypy; expected all pass.

## Task 2: Installed integration, review and local handoff

**Files:** shared client README and EP README; bilateral fixture/tests as needed;
AO `backend/uv.lock`, `backend/requirements.txt`, `backend/ep-client-source.json`,
`backend/artifacts.lock.yaml`.

**Consumes:** shared listener/scheduler from Task 1, no AO-specific dispatch logic.

- [x] Refresh dependency locks and run both `make install` targets.
- [x] Run full EP suite against disposable PostgreSQL, lint/format/strict types.
- [x] Build EP image; run affected AO unit/integration and installed bilateral
  tests. Run AO canonical formatting/type checks and report baseline gate failures.
- [x] Perform one independent whole-change review; fix material findings with
  failing regressions, then run covering checks.
- [x] Commit EP implementation locally; generate AO's client archive/pin from
  that revision and verify checksum/extraction and source-package tests.
- [x] Commit AO updates locally, record validation, clean task-owned test resources.
