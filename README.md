# Retry Window Lab

A Python distributed task queue for studying recovery when an external
provider's idempotency protection expires.

## Stack

- Python 3.12
- PostgreSQL
- Redis Streams
- Ubuntu / WSL2
- Docker Compose

## Current status

Phase 10 compares three retry policies across eight provider fault scenarios.
All 367 tests passed on Ubuntu / WSL2. In the 24-trial comparison, the
evidence-based policy confirmed six single-reservation completions with zero
extra reservations. The fresh-key and same-key policies produced five and
four extra reservations, respectively.

## Local setup

Keep Docker Desktop running with Ubuntu WSL integration enabled.

    python3 -m venv .venv
    source .venv/bin/activate
    python -m pip install -r requirements.lock
    python -m pip install --no-deps -e .

Create .env from .env.example and replace the placeholder password before
first database initialization.

    docker compose up -d --wait --wait-timeout 120
    python -m retry_window_lab.provision
    python -m pytest -q

## Useful commands

    docker compose ps
    docker compose logs --tail=60 postgres redis
    docker compose stop
    docker compose up -d --wait

PostgreSQL: localhost port 55432.
Redis: localhost port 56379.

Named volumes retain local data. Database bootstrap credentials are for
local development. Restricted application/provider roles will be introduced
before provider isolation is implemented.

## Reproducibility

requirements.lock records resolved Python dependencies.
docs/INFRASTRUCTURE_IMAGES.txt records downloaded image references.
Compose tags select a release series and may receive updates.

## Specification

See docs/RETRY_WINDOW_LAB_SPEC.md.

## Local demonstration

    python -m retry_window_lab.demo

See docs/LOCAL_EXECUTION.md for behavior and limitations.

## API and dispatcher

Start the API:

    python -m uvicorn retry_window_lab.api:app --host 127.0.0.1 --port 8000

In a separate terminal, publish pending notifications:

    python -m retry_window_lab.dispatcher --once

For continuous publication:

    python -m retry_window_lab.dispatcher

Interactive API documentation: http://localhost:8000/docs

See docs/DURABLE_SUBMISSION.md and docs/API_AND_DELIVERY.md.

## Workers

Run each worker in a separate terminal:

    python -m retry_window_lab.worker --worker-id worker-a
    python -m retry_window_lab.worker --worker-id worker-b

Publish accepted work using the dispatcher. Scan expired attempts manually:

    python -m retry_window_lab.recover

Expired attempts are recorded as uncertain; configured reservation tasks
enter provider-aware recovery.
See docs/WORKERS.md for ownership, acknowledgement and recovery behavior.

## Provider simulator

Provision storage and restricted runtime credentials before starting services:

    python -m retry_window_lab.provision

Start the provider:

    python -m uvicorn retry_window_lab.provider_api:app --host 127.0.0.1 --port 8001

Run the demonstration from another terminal:

    python -m retry_window_lab.provider_demo

The demonstration intentionally creates duplicate effects after protection
expires. See docs/PROVIDER_SIMULATOR.md for contracts and limitations.

## Reservation recovery

With the provider running, execute the recovery demonstration:

    python -m retry_window_lab.recovery_demo

Bind provider context before executing a reservation task.
The task API exposes provider-context binding and recovery status endpoints.

See docs/RECOVERY.md for contracts, budgets, scheduling and limitations.

Browser interfaces:

- Task API documentation: http://127.0.0.1:8000/docs
- Provider documentation: http://127.0.0.1:8001/docs

Start the corresponding API service before opening its documentation.
The task API serves the queue dashboard at http://127.0.0.1:8000/dashboard.

## Queue capacity and Redis recovery

Run the isolated concurrency demonstration:

    python -m retry_window_lab.capacity_demo

After Redis loss, rebuild ready notifications from PostgreSQL:

    python -m retry_window_lab.dispatcher --replay-ready

See [Capacity and concurrency](docs/CAPACITY.md) for worker slots,
admission limits and replay behavior.

## Queue monitoring

Start the task API and open http://127.0.0.1:8000/dashboard.
With the provider running, launch the isolated dashboard demo:

    python -m retry_window_lab.monitoring_demo

See [Monitoring](docs/MONITORING.md) for metric definitions, task history
and structured log correlation.

## Retry policy comparison

With the provider simulator running, generate the comparison report:

    report_dir=$(mktemp -d /tmp/rwl-comparison.XXXXXX)
    python -m retry_window_lab.comparison --seed 42 --output "$report_dir/results"

See [Retry policy comparison](docs/POLICY_COMPARISON.md) for the scenario
matrix, measurement definitions and verified results.
