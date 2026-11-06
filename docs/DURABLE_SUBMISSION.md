# Durable submission — Phase 4.1–4.3

## Scope

These subphases add versioned PostgreSQL migrations, durable task acceptance,
submission deduplication and transactional outbox records. Redis publication,
HTTP endpoints and worker execution follow in subsequent subphases.

No third-party dependency change is required.

## Commands

From the project root with the virtual environment active:

```bash
docker compose up -d --wait --wait-timeout 120
python -m retry_window_lab.migrate
python -m retry_window_lab.migrate
python -m retry_window_lab.storage_demo
python -m pytest -q
```

The first migration run applies version 1. Repeated runs report no pending
migrations. The demo persists a checksum task in QUEUED with no result. A
matching resubmission returns the original task and operation identifiers.
The first run reports created_on_this_call=true; later runs report false.
Both matching_resubmission_reused_task and readable_through_new_connection
must be true. This demonstrates new database connections, not crash recovery.

## Files

- db.py: dedicated schema validation, quoted identifiers and connection settings.
- migrate.py: version 1 DDL and transactional migration runner.
- storage.py: committed task snapshots and repository operations.
- storage_demo.py: durable submission and readback demonstration.
- tests/integration/test_durable_submission.py: 23 additional test cases.

## PostgreSQL schema

The default namespace is rwl, with three tables:

| Table | Responsibility |
| --- | --- |
| schema_migrations | Applied migration versions, checksums and timestamps |
| tasks | Validated task definition, submission key, identities, current state and result |
| outbox | Ready-notification intent, event identity, delivery generation and publication status |

Task state begins at QUEUED and delivery_generation at 1. One unique outbox
event exists per task, delivery generation and event type. The unpublished
index supports later dispatcher polling.

Attempt persistence and worker leases are not part of this migration; introduce
them in later migrations during distributed execution work.

The local database bootstrap account is still used for development. Database
schema separation in these tests is cleanup isolation, not a production
authorization boundary. Restricted application/provider accounts remain pending.

## Acceptance transaction

1. A validated TaskDefinition enters TaskRepository.submit.
2. The repository attempts INSERT ... ON CONFLICT (submission_key) DO NOTHING.
3. For a new task, it inserts the TASK_READY outbox event in the same transaction.
4. For an existing key, a separate SELECT reads the committed task and compares
   its fingerprint. Matching work returns the existing task; conflicting work
   raises SubmissionConflict.
5. The repository returns acceptance only after successful commit.

The database unique constraint arbitrates competing submissions. PostgreSQL
Read Committed gives the separate SELECT a fresh statement snapshot after a
conflicting INSERT waits. Do not collapse this into a same-statement CTE
fallback without reconsidering visibility under concurrent submissions.

If outbox insertion fails, the task insert rolls back. The integration suite
injects this failure with a database trigger in an isolated test schema.
Database connectivity failures propagate; they never become accepted tasks.

If a commit succeeds but its response is lost, the caller may receive an error
despite acceptance. Resubmit the same submission key and business parameters to
resolve that uncertainty. A new key represents different work and can create
another task.

This repository uses synchronous connections and explicit transaction contexts.
Each operation obtains its own connection. Connection pooling and asynchronous
I/O can be introduced when needed; no performance claim is made here.

## Outbox meaning

An outbox row is a durable intent to publish a ready-work notification. Its
presence does not mean Redis received it, a worker ran or an external operation
succeeded. published_at remains NULL until a dispatcher confirms publication.

The current database transaction does not include Redis. Subphase 4.4 handles
publication failures and possible duplicate notifications after a publish/mark
crash. Recovery from a complete Redis reset is separate remaining work.

## Migration rules

- Migration DDL and its ledger row commit together.
- A transaction-scoped PostgreSQL advisory lock serializes migrators for the
  same schema before schema creation.
- Applied migration text must remain unchanged. Add later changes as new
  migration versions.
- Checksum mismatch, unknown versions and nonsequential versions fail explicitly.
- Schema and table identifiers are quoted; application data uses parameters.
- Connections use Read Committed, UTC result timestamps, a 5-second lock
  timeout and a 10-second statement timeout.

The migration SQL is embedded in migrate.py so it remains part of the Python
package without a separate resource-packaging change. This is a small versioned
migration mechanism, not a general-purpose migration framework.

## Test isolation and expected checks

The new test module contains 23 cases. Each database fixture uses a fresh
rwl_test_<uuid> schema, then drops only that schema. Default rwl data is preserved.
Migration-race tests also use fresh isolated schemas.

Coverage includes repeated and simultaneous migrations, checksum/version
rejection, transactional DDL rollback, atomic task/outbox acceptance, fresh
connection readback, matching/conflicting submissions, different submission
keys, outbox-write rollback, simultaneous matching/conflicting submissions,
unknown task lookup, queued reservations, database failure and schema validation.

Verified on Ubuntu / WSL2: all 23 durable-submission cases passed.
After API and dispatcher integration, the complete suite passed 131 tests.
See API_AND_DELIVERY.md for the additional delivery verification.

## References

- https://www.postgresql.org/docs/17/transaction-iso.html
- https://www.postgresql.org/docs/17/sql-insert.html
- https://www.psycopg.org/psycopg3/docs/basic/transactions.html

The transaction boundaries and repository behavior above are project design
decisions using the documented database/driver mechanisms.
