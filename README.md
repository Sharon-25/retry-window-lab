# Retry Window Lab

Retry Window Lab is a Python distributed task queue that explores a failure
mode in external APIs. A provider can commit an operation, lose the response
and later forget its idempotency key. A resend after that window can apply
the same operation twice.

The queue records task state, attempts and recovery decisions in PostgreSQL.
A transactional outbox publishes work through Redis Streams. Workers use
leased claims, and completion checks reject stale owners. For uncertain
reservations, the recovery engine reconciles provider evidence, retries
after a proven no-effect outcome or sends the task to `NEEDS_REVIEW`.

## Results

In the [24-trial policy comparison](docs/POLICY_COMPARISON.md), fresh-key
retries produced five extra reservations, same-key retries produced four,
and evidence-based recovery produced zero. When lookup evidence ran out,
the evidence policy left the task for review.

In the [WSL2 checksum benchmark](docs/PERFORMANCE.md), four worker slots
completed a median **28.408 tasks/s** versus **11.498 tasks/s** with one slot.
Durable p95 latency fell from **7,045 ms to 993 ms**. These figures describe
the documented single-host workload. The full Phase 11 suite passed 369 tests.

## Local setup

Use Python 3.12, Docker Compose and Ubuntu or WSL2. On WSL2, start Docker
Desktop and enable integration for your distribution.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.lock
python -m pip install --no-deps -e .
test -f .env || cp .env.example .env
```

Set a local bootstrap password in `.env` before starting PostgreSQL for
the first time. Provisioning creates the schemas and separate runtime roles
for the task queue and provider. Keep `.env` local.

```bash
docker compose up -d --wait --wait-timeout 120
python -m retry_window_lab.provision
python -m pytest -q
```

Default ports are 55432 for PostgreSQL and 56379 for Redis. Docker Compose
retains data in named volumes. `docker compose stop` stops the services and
keeps the volumes.

## Run the queue

With the virtual environment active, run each line in its own terminal.

```bash
python -m uvicorn retry_window_lab.api:app --host 127.0.0.1 --port 8000
python -m retry_window_lab.dispatcher
python -m retry_window_lab.worker --worker-id worker-a --concurrency 2
```

Submit a checksum from another terminal.

```bash
curl -sS -X POST http://127.0.0.1:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"kind":"checksum","submission_key":"readme-checksum-1","payload":{"text":"abc"}}'
```

The response includes the task ID. Read its state and result at
`GET /tasks/{task_id}` or open the [queue dashboard](http://127.0.0.1:8000/dashboard).
Use the [task API docs](http://127.0.0.1:8000/docs) to inspect the routes.

## Explore the failure cases

Start the provider simulator in a separate terminal.

```bash
python -m uvicorn retry_window_lab.provider_api:app --host 127.0.0.1 --port 8001
```

Run the six [reservation recovery scenarios](docs/RECOVERY.md).

```bash
python -m retry_window_lab.recovery_demo
```

Use the [release verification guide](docs/RELEASE.md) to reproduce the suite,
recovery and capacity demos, comparison and benchmark smoke run in isolation.

## Design and operations

| Topic | Documentation |
| --- | --- |
| Contracts and failure model | [Specification](docs/RETRY_WINDOW_LAB_SPEC.md) |
| Durable submission and Redis delivery | [Submission](docs/DURABLE_SUBMISSION.md), [API and delivery](docs/API_AND_DELIVERY.md) |
| Leases and acknowledgements | [Workers](docs/WORKERS.md) |
| Provider profiles and recovery | [Simulator](docs/PROVIDER_SIMULATOR.md), [Recovery](docs/RECOVERY.md) |
| Admission, concurrency and Redis replay | [Capacity](docs/CAPACITY.md) |
| Dashboard and correlated task history | [Monitoring](docs/MONITORING.md) |
| Experiments and measurement | [Policy comparison](docs/POLICY_COMPARISON.md), [Performance](docs/PERFORMANCE.md) |
| Verification history | [Engineering backlog](docs/ENGINEERING_BACKLOG.md) |

The lab is intended for local use. Its recovery behavior depends on the
provider contracts modeled in the simulator. PostgreSQL storage is the
durable source of task state, and Redis notification replay starts from that
state. See the [release limitations](docs/RELEASE.md#scope-and-limitations)
before applying the design to another environment.
