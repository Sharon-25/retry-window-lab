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

Phase 4: durable task submission, transactional outbox, Redis Streams
publication and submission/status API implemented and verified.

131 tests passed: 97 unit tests and 34 integration tests.
Independent worker execution follows in Phase 5.

## Local setup

Keep Docker Desktop running with Ubuntu WSL integration enabled.

    python3 -m venv .venv
    source .venv/bin/activate
    python -m pip install -r requirements.lock
    python -m pip install --no-deps -e .

Create .env from .env.example and replace the placeholder password before
first database initialization.

    docker compose up -d --wait --wait-timeout 120
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
