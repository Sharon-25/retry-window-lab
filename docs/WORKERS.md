# Distributed workers — Phase 5

## Components

- `ownership.py`: atomic PostgreSQL claims, attempt records, lease renewal,
  conditional completion and expired-attempt detection.
- `worker.py`: Redis consumer group, pending-entry reclamation and one synchronous
  checksum handler at a time per process. Separate worker processes run concurrently.
- `recover.py`: one bounded scan for expired attempts.
- Migration 2 adds ownership columns and attempts. Migration 1 is unchanged.

No new application dependency is required.

## Apply and verify

With the virtual environment active and PostgreSQL/Redis healthy:

```bash
python -m retry_window_lab.migrate
python -m retry_window_lab.migrate
python -m pytest -q tests/unit
python -m pytest -q tests/integration/test_workers.py
python -m pytest -q
```

An existing Phase 4 database applies `[2]` once. A new database applies `[1, 2]`.
The second migration run has no pending work.

Verified on Ubuntu / WSL2 with PostgreSQL 17 and Redis 7.4:
142 unit tests and 59 integration tests passed, for 201 total.
All 25 worker integration cases passed. Migration 2 applied successfully;
a second migration run reported no pending migrations.

The manual demonstration ran two worker processes and verified eight tasks
with correct checksum results and exactly eight successful attempts.

Existing upstream test-client warnings do not alter the test assertions.

## Run

Keep the API and dispatcher from Phase 4 available as needed. In separate
Ubuntu terminals, from the repository root with the virtual environment active:

```bash
python -m retry_window_lab.worker --worker-id worker-a
```

```bash
python -m retry_window_lab.worker --worker-id worker-b
```

Publish accepted work with `python -m retry_window_lab.dispatcher --once`, or
leave the dispatcher running. Workers log task identifiers and bounded statuses.
`--once` handles at most one notification and exits; it does not drain a backlog.
Ctrl+C stops a worker. Interrupting an executing handler leaves the durable
attempt RUNNING until its lease expires and recovery records uncertainty.

Run a recovery scan without starting another worker:

```bash
python -m retry_window_lab.recover
```

The continuous worker loop also scans expired attempts between tasks. There is
no dedicated scanning thread; an operator can run the recovery command when
all workers are unavailable or busy.

## Ownership contract

A worker locks the task row, validates the notification against its persisted
outbox event and current delivery generation, verifies QUEUED eligibility,
increments the monotonic lease token, creates an attempt and changes the task
to RUNNING in one transaction. A duplicate notification creates no new attempt.
Task, operation and attempt identifiers remain distinct.

Only a matching active attempt, current token and RUNNING task with an unexpired
PostgreSQL lease may renew or finish. The lease is checked using database time
after acquiring the task row lock. Completion updates the task and attempt in
one transaction. No database transaction spans handler execution or Redis I/O.
All ownership writers acquire the task lock before touching its attempt.

The default lease is 30 seconds, configurable with `--lease-seconds` in 3..3600.
A heartbeat thread renews every one-third of that interval. On renewal error or
rejection, the worker discards its completion and lets recovery resolve the
expired attempt. A heartbeat cannot cancel an already executing handler or fence
an external provider. The database completion guard is the final authority.
These settings are operational defaults, not real-time guarantees.

## Notification contract

The default stream is `rwl:ready`; the consumer group is `rwl:workers`, created
at `0-0` so previously published entries can be consumed. Redis clients decode
responses to strings. A notification must contain exactly protocol_version,
event_id, task_id, delivery_generation and event_type, with protocol version 1,
valid UUIDs, a positive signed-64-bit generation and TASK_READY type.

Workers acknowledge after a database claim commits, or after PostgreSQL confirms
that the notification is obsolete or already represented by durable state.
Acknowledgement does not mean successful execution. If acknowledgement fails
after claiming, this implementation does not begin the handler; the task remains
RUNNING for recovery. That choice can sacrifice automatic progress to preserve
an explicit uncertainty record.

Pending entries are scanned with XAUTOCLAIM, maintaining the scan cursor, at a
60-second Redis idle threshold. Reclaimed notifications are revalidated against
PostgreSQL. Redis idle age never grants execution ownership. Malformed entries
are logged as invalid_notification and acknowledged without invoking a handler.
There is no dead-letter archive yet. Do not trim the stream or delete its group
while relying on pending-entry recovery.

Reservation tasks have no provider handler in this phase. They remain QUEUED;
their notifications remain pending and workers report unsupported. This avoids
claiming an external operation before the provider contract is implemented.

## Outcomes and recovery

A valid JSON-object checksum result becomes SUCCEEDED. KnownTaskFailure is an
explicit permanent, no-effect rejection and becomes FAILED / KNOWN_FAILURE.
Unexpected exceptions or invalid results become NEEDS_REVIEW / UNKNOWN.
Exception messages are not persisted in task or attempt error records.

An expired RUNNING attempt becomes UNKNOWN with LEASE_EXPIRED, and the task
becomes NEEDS_REVIEW. The scanner does not retry, increment delivery generation
or publish another notification. Expiration cannot prove whether a handler
ran or an external effect occurred. Provider-aware reconciliation and retry
policies follow in later phases. No exactly-once external execution is claimed.

## Verification boundaries

Worker integration tests use isolated schemas and streams. They cover competing
claims, separate worker processes completing eight tasks, duplicate notifications,
reclaimed pending entries, heartbeat renewal, stale tokens/attempts, expiration,
atomic rollback and an upgrade from migration 1 preserving tasks and outbox.
A subprocess uses os._exit immediately after a committed claim to test abrupt
process loss. Expiry tests move stored lease deadlines into the past explicitly;
they do not wait through a 30-second lease or simulate provider time.

The tests do not establish recovery of Redis data loss, external-effect safety,
automatic retry, production throughput or network-partition fault tolerance.
Worker leases and result guards preserve database ownership; they do not prevent
an old external request from arriving after ownership is lost.
