# Worker concurrency, queue capacity and Redis recovery

## Setup and verification

Stop old application processes and run the Phase 8 installer from the repository root.
Then apply the queue migration and update the application role's grants.

```bash
python /mnt/c/Users/Shax/Downloads/retry_window_lab_phase8_capacity.zip
docker compose up -d --wait --wait-timeout 120
python -m retry_window_lab.migrate
python -m retry_window_lab.migrate
python -m retry_window_lab.provision
python -m pytest -q tests/integration/test_capacity.py
python -m pytest -q
python -m retry_window_lab.capacity_demo
```

An existing Phase 7 database applies migration 4. The second run reports no
pending migrations. The application role can read the queue setting.
The bootstrap operator account controls changes to the limit.

Verified on Ubuntu / WSL2 with Docker Compose. Migration 4 applied
successfully, and the second run reported no pending migrations.
All eight Phase 8 integration tests passed, with 359 tests passing overall.

The live demonstration observed four active execution slots, rebuilt six
notifications and injected six duplicates. Eight tasks completed and two
interrupted tasks entered NEEDS_REVIEW. Recovery progressed while both
execution slots on the surviving worker were occupied.

## Admission limit

The limit defaults to 1,000 outstanding tasks per queue schema. It includes
QUEUED, RUNNING, RETRY_WAIT, RECONCILING and NEEDS_REVIEW. SUCCEEDED and FAILED
release capacity. A reservation waiting for provider context also occupies a
slot because the API has already accepted it.

```bash
python -m retry_window_lab.capacity
python -m retry_window_lab.capacity --limit 2000
```

The setting persists in PostgreSQL and applies to all application API instances
using the schema. The limit range is 1 through 1,000,000. Lowering it below the
current outstanding count pauses new acceptance until enough tasks finish.
Existing tasks keep their states and recovery budgets.

A transaction-scoped advisory lock serializes admission and capacity changes
within the schema. After acquiring it, submission checks the idempotency key
before counting outstanding rows. New task and outbox writes commit together.
An index covers the outstanding states and the count stops at the configured
limit. Every submitting API process must run this admission code.

| Outcome | API response |
| --- | --- |
| Accepted new submission | 202 |
| Same submission key and fingerprint | 200, including at capacity |
| Same submission key with different work | 409 |
| New work while full | 429 with queue_at_capacity and Retry-After: 1 |
| PostgreSQL unavailable or admission lock times out | 503 with Retry-After: 1 |

Redis can be unavailable during submission. The accepted task and its outbox
intent remain durable until the dispatcher can publish.

## Worker slots and distribution

Start the dispatcher in one terminal. Run the worker commands in separate
terminals with distinct worker IDs.

```bash
python -m retry_window_lab.dispatcher
python -m retry_window_lab.worker --worker-id worker-a --concurrency 2
python -m retry_window_lab.worker --worker-id worker-b --concurrency 2
```

The default is one execution slot per process. The allowed range is 1–8.
Each slot has its own Redis consumer identity and pulls only when it can execute
another task. The two commands above allow up to four simultaneous executions.
Distribution depends on which slots are available when notifications arrive.

Each process also reserves one lane for expiry scanning, due retry scheduling
and one reconciliation observation per pass. A pass schedules and expires at
most 100 tasks each and pauses for a quarter second before starting again.
An asyncio loop schedules the slots and offloads the existing synchronous I/O
to a bounded thread executor sized for the execution slots plus recovery.
Heartbeats continue to use their existing per-attempt threads.

Ctrl+C and SIGTERM stop new reads and allow in-flight operations to finish.
An abrupt process exit leaves its durable attempts for lease recovery.
The one-notification command uses one execution slot.

## Restore notifications after Redis loss

Once Redis is available, run the following command.

```bash
python -m retry_window_lab.dispatcher --replay-ready
```

Replay materializes retries due at the start of the command, then republishes
current ready outbox events in batches. It selects QUEUED checksums and QUEUED
reservations with provider context. Existing published timestamps remain intact.
Running or terminal tasks, obsolete delivery generations and future retries
stay outside the replay. Normal workers and the dispatcher handle work that
becomes ready after the pass.

Run the command again if a Redis error interrupts it. Replaying an event can
create duplicate stream entries. Workers still claim the current generation
through PostgreSQL, so a duplicate cannot acquire another current attempt.
A running worker that encounters a missing group recreates it from the
beginning of the stream and revalidates every notification through PostgreSQL.
The replay command is the operator action for lost stream contents.

## Demonstration

Run `python -m retry_window_lab.capacity_demo` with PostgreSQL and Redis running.
The command creates its own schema and stream and cleans both up after the run.
It starts two separate worker processes, each with two slots. Four checksum
handlers pause at a controlled gate while six more tasks wait. The harness
deletes its own stream, rebuilds six notifications and replays them again to
inject duplicates.

It stops one demo process abruptly and observes that the surviving process
recovers the two expired attempts while both of its execution slots are busy.
After the gate opens, eight tasks complete and two remain in NEEDS_REVIEW.
The expected attempts by process are two for demo-a and eight for demo-b.
This distribution follows from the controlled startup and interruption sequence.
