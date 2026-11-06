# Provider simulator — Phase 6

## Setup

From the repository root, with the virtual environment active and containers healthy:

```bash
python -m retry_window_lab.provider_migrate
python -m retry_window_lab.provider_migrate
python -m retry_window_lab.provision
python -m pytest -q tests/unit
python -m pytest -q tests/integration/test_provider.py
python -m pytest -q
```

The provider has its own schema (`rwl_provider`) and version ledger. Its first
migration applies `[1]`; the second run reports no pending migrations. Queue
migrations 1 and 2 remain unchanged. Existing task, attempt and outbox data are
preserved. A fresh installation must apply the application migrations as well;
the provision command applies both migration sets before granting access.

Provisioning generates two passwords in the ignored `.env` file when absent.
It does not print passwords. It can be rerun with the same configuration.
Restart existing API, dispatcher and worker processes after provisioning so
that their new runtime connection code is loaded.

No new application dependency is required.

Verified on Ubuntu / WSL2 with PostgreSQL 17 and Redis 7.4:
175 unit tests and 98 integration tests passed, for 273 total.
All 39 provider integration cases passed. Provider migration and restricted
runtime-role provisioning completed successfully.

The live HTTP demonstration passed all six checks: protected reuse,
duplicate effects after expiry, committed effects despite response loss,
delayed visibility, unavailable lookup and late original-request commitment.

## Run the provider

In a terminal with the virtual environment active:

```bash
python -m uvicorn retry_window_lab.provider_api:app --host 127.0.0.1 --port 8001
```

Documentation: http://127.0.0.1:8001/docs
Liveness: http://127.0.0.1:8001/health

In another terminal:

```bash
python -m retry_window_lab.provider_demo
```

All six demonstration checks should print true. The demonstration creates five
fresh experiment namespaces, performs mutations and lookups over HTTP, and uses
operator-only database controls to advance simulated time and inject faults.
It deliberately demonstrates duplicate effects with an unsafe retry. It does
not implement the queue's recovery policy.

## Database roles and isolation

| Component | Credentials | Permissions |
| --- | --- | --- |
| Migration, provisioning, tests, experiment operator | Existing PGUSER / PGPASSWORD | Bootstrap administration |
| Task API, dispatcher, worker, recovery scanner, storage demo | RWL_APP_USER / RWL_APP_PASSWORD | SELECT, INSERT, UPDATE on queue tasks, attempts and outbox |
| Provider HTTP service | RWL_PROVIDER_USER / RWL_PROVIDER_PASSWORD | SELECT, INSERT, UPDATE on provider data tables |

Default runtime roles are `rwl_app` and `rwl_provider_service`. Neither owns a
schema, database or table, inherits role memberships, or has superuser, role
creation, database creation, replication or bypass-RLS attributes. Provisioning
refuses to adopt an elevated, owning or member role. Runtime credentials cannot
read the other schema, switch into the other runtime role, or apply migrations.
Migration ledgers are not granted to runtime roles. Schemas and data tables have
no PUBLIC access. Future tables need explicit grants in provisioning.

Integration tests connect as the actual runtime roles to prove allowed queue
execution/provider operations and rejected cross-schema reads, role switching
and object creation. Tests retain bootstrap credentials to create isolated
schemas and temporary roles, which are removed afterward.

This is PostgreSQL privilege isolation in a trusted local lab. The shared local
`.env` contains bootstrap and both runtime credentials; it is not OS-level secret
isolation against a malicious process that can read that file or environment.
Operator/demo code intentionally has provider ground-truth access. Queue runtime
modules do not import provider storage or use bootstrap connections.

## Reservation contract

POST `/reservations` accepts exactly:

```json
{
  "experiment_id": "11111111-1111-4111-8111-111111111111",
  "operation_id": "22222222-2222-4222-8222-222222222222",
  "idempotency_key": "reservation-key",
  "sku": "SKU-BOOK-001",
  "quantity": 2
}
```

Create a real experiment first; the IDs above only illustrate the shape.
Each experiment starts with 100 BOOK and 100 PEN units. Known SKUs and positive
signed-32-bit quantities are validated; booleans, extra fields and blank keys
are rejected. The provider fingerprint hashes schema version 1, SKU and quantity
using the project's compact sorted JSON encoding. It is a provider request
fingerprint, distinct from the queue definition fingerprint that includes task
kind and provider profile.

A PostgreSQL transaction locks the experiment, creates a request receipt,
checks protection, decrements stock, creates a reservation and stores its key
protection atomically. Experiment-level serialization keeps this simulator
simple and prevents overselling; it is not a throughput optimization.
No transaction spans HTTP delivery. Every POST creates a new receipt, so receipt
identity does not accidentally grant permanent mutation deduplication.

Protection is scoped by experiment namespace, operation ID and idempotency key.
A matching retry before expiry reuses the reservation without reducing stock or
extending the expiry. Conflicting business data under a protected key returns
HTTP 409. Protection starts when the reservation is accepted. The default
window is 60 provider-seconds. At exactly the expiry time, protection is over:
a new request can create another reservation, while old reservation records
remain. Another key can create another effect even within a protection window.
These boundaries are deliberate experimental behavior.

Successful and known-rejected requests return HTTP 200 with status SUCCEEDED or
REJECTED. Insufficient stock is a known no-effect rejection for that request.
Delayed requests return HTTP 202 / PROCESSING with no acceptance/expiry time yet.
Replies include request/operation identity, fingerprint, profile version,
window, acceptance and expiry timestamps where available. Permanent reservation
records allow duplicate effects to be observed instead of hidden.

## Lookup profiles

GET `/experiments/{experiment_id}/operations/{operation_id}` returns visible
reservations for that operation, preserving multiple reservations when present.

| Profile | Behavior |
| --- | --- |
| authoritative | Immediate committed reservations; request receipts expose PROCESSING or settled statuses |
| delayed | Reservations appear only when provider time reaches their visibility deadline; request receipts are not exposed |
| unavailable | Lookup returns HTTP 503; mutations still operate |

NOT_FOUND always includes `absence_authorizes_retry: false`. A request can still
be delayed and later commit. Even authoritative absence does not establish that
all old dispatched requests are permanently settled with no effect.

## Operator controls

```bash
python -m retry_window_lab.provider_admin create --profile authoritative
python -m retry_window_lab.provider_admin create --profile delayed --visibility-delay 10
python -m retry_window_lab.provider_admin create --profile unavailable
```

Use the returned experiment UUID in these examples:

```bash
python -m retry_window_lab.provider_admin advance EXPERIMENT_UUID --seconds 60
python -m retry_window_lab.provider_admin inject EXPERIMENT_UUID --fault RESPONSE_LOST
python -m retry_window_lab.provider_admin inject EXPERIMENT_UUID --fault REJECT_NO_EFFECT
python -m retry_window_lab.provider_admin inject EXPERIMENT_UUID --fault DELAY_DELIVERY --delay-seconds 130
```

Controls are CLI-only; the HTTP service exposes no clock or injection routes.
Each configured fault is consumed by the next POST in that experiment. Keep an
experiment's clients coordinated when injecting faults; a concurrent request
can consume the next-request fault.

- RESPONSE_LOST commits the operation, then HTTP returns a deterministic 503
  without receipt/reservation evidence. This models post-commit ambiguity; it
  does not simulate a literal broken TCP connection or OS process crash.
- REJECT_NO_EFFECT settles that request as rejected without creating an effect.
  Earlier requests for the operation may still have effects.
- DELAY_DELIVERY persists a pending original request; its later application can
  occur after a new request's protection expires.

Time is a per-experiment integer clock starting at zero. Advancing time settles
all due pending requests at the new clock value, ordered by due time and receipt
ID. If an advance jumps beyond a due time, acceptance is at the new clock value,
not retroactively at the due time. There is no background virtual-clock timer.
Provider clock changes never alter PostgreSQL worker lease time.

## Verification and remaining work

Tests cover migration consistency, request validation, all profiles, exact expiry
and visibility boundaries, conflicting protected payloads, late original
requests, one-shot faults, post-commit response loss, concurrent deduplication,
stock consistency, atomic rollback, fresh experiment stock and database roles.

Queue workers continue to execute checksum tasks. Reservation-to-provider
execution, durable observations, evidence-based decisions, retry scheduling and
comparative policies follow in Phase 7. Phase 6 does not automatically retry an
ambiguous operation or claim exactly-once external execution.

The provider is an unauthenticated localhost experiment service. Keep it bound
to loopback. Provider controls and inspection are trusted test tools; this phase
does not establish a production service security boundary or benchmark.
