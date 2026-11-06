# API and outbox delivery — Phase 4.4–4.6

## Start

With the project environment active and PostgreSQL/Redis healthy:

```bash
python -m retry_window_lab.migrate
python -m uvicorn retry_window_lab.api:app --host 127.0.0.1 --port 8000
```

Run API client commands or the dispatcher in a second Ubuntu terminal with the
same project directory and virtual environment activated.

```bash
python -m retry_window_lab.dispatcher --once
python -m retry_window_lab.dispatcher
```

The first command processes one bounded batch. The second polls continuously;
Ctrl+C stops it. Default batch size: 100; configurable range: 1–1000. Default
poll interval: one second; minimum: 0.1 seconds. This is publication polling,
not task execution or provider-time simulation.

## Endpoints

| Method and path | Behavior |
| --- | --- |
| GET /health | Process liveness only; does not verify infrastructure readiness |
| POST /tasks | Validate and durably accept work |
| GET /tasks/{task_id} | Retrieve current durable task state and result |
| GET /docs | Generated interactive API documentation |

Submission example:

```json
{"kind":"checksum","submission_key":"api-checksum-001","payload":{"text":"abc"}}
```

- HTTP 202: new task committed with its outbox event.
- HTTP 200: matching submission reused an existing task.
- HTTP 409: submission key reused for different work.
- HTTP 422: malformed input, invalid UUID or invalid task definition.
- HTTP 404: valid task UUID does not exist.
- HTTP 503: operational database failure; acceptance may be uncertain if a
  commit response was lost. Resolve by resubmitting the same key and parameters.

POST responses include a Location header with the relative task-status URL and
a JSON body containing created and task. GET returns the task object directly.

The API uses synchronous route functions because its current PostgreSQL driver
calls are synchronous. FastAPI runs these routes through its thread pool. It
does not run a dispatcher or task handler inside an HTTP request.

The API needs PostgreSQL to accept work, but does not call Redis. A Redis outage
leaves committed outbox intent for a later dispatcher retry. Authentication,
multi-user authorization and HTTP admission/backpressure are not implemented
yet; this development API binds to localhost.

## Delivery protocol

Ready stream: rwl:ready.

Each Redis entry contains these string fields:

```json
{
  "protocol_version": "1",
  "event_id": "<outbox event UUID>",
  "task_id": "<task UUID>",
  "delivery_generation": "1",
  "event_type": "TASK_READY"
}
```

Redis generates the stream-entry ID. event_id identifies the durable logical
notification and stays the same if that notification is published again.
The business payload remains in PostgreSQL rather than being copied into Redis.

## Publication sequence and failure behavior

1. Select a bounded batch of unpublished outbox rows.
2. Publish each row with XADD outside any PostgreSQL transaction or row lock.
3. After Redis acknowledges XADD, mark that row published in an autocommit
   PostgreSQL UPDATE, conditional on published_at still being NULL.

There is no atomic transaction across PostgreSQL and Redis:

- Failure before publication leaves the row pending.
- An ambiguous Redis response may mean the event was published; retry can
  duplicate it.
- Failure after publication but before marking leaves the event pending and
  can also duplicate it on retry.
- Concurrent dispatchers may select and publish the same event. Their
  conditional UPDATE prevents overwriting an already-set publication timestamp.
- Earlier successfully marked events remain marked if a later batch event fails.
- No workers run yet. QUEUED tasks remain QUEUED after publication.

This initial design accepts duplicate notifications rather than introducing
dispatcher leases or holding row locks during network calls. It is not an
exactly-once publication mechanism. Worker-side database eligibility and attempt
ownership checks belong in Phase 5. Redis retention/reset recovery and bounded
stream cleanup remain separate engineering work. An acknowledged Redis write
does not establish survival of every possible persistence/storage failure.

CLI reports selected, notifications_sent and rows_marked for successful batches.
Concurrent duplicate publishers can have notifications_sent greater than
rows_marked. On errors, the CLI emits a bounded exception type rather than raw
connection details. A --once error exits nonzero. Continuous mode retries after
its polling interval; partial progress is inspected in PostgreSQL.

## Tests and verification

20 additional unit cases cover API status codes, validation before storage,
sanitized database errors, task lookup, liveness and dispatcher batch limits.
The full unit suite contains 97 cases and passed during source preparation.

Nine additional integration cases use fresh database schemas and unique Redis
keys. They cover publication, restart after a successful mark, unavailable
Redis, failure after publication but before marking, batch limits, partial
batch failure, concurrent publishers and API-to-outbox-to-stream behavior.
They do not flush a shared Redis database or delete default application data.

Verified on Ubuntu / WSL2: 97 unit tests and 34 integration tests passed,
for 131 total. Manual verification confirmed HTTP 202 acceptance, HTTP 200
matching resubmission, task retrieval, Redis publication and an empty
subsequent dispatcher batch.

The publish/mark test injects a real database UPDATE failure after a real Redis
write. This exercises the crash window's observable state; it does not claim
to have performed an OS-level kill. Independent process-kill testing remains
part of later fault-testing work.

## Dependency additions

- fastapi>=0.115,<1
- pydantic>=2.9,<3
- uvicorn>=0.30,<1
- Development: httpx>=0.27,<1

Record the resolved versions in requirements.lock after installation. No
database migration change is required for these subphases.

## References

- https://fastapi.tiangolo.com/async/
- https://fastapi.tiangolo.com/tutorial/testing/
- https://fastapi.tiangolo.com/advanced/additional-status-codes/
- https://redis.io/docs/latest/commands/xadd/
