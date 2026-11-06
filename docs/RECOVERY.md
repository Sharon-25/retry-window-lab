# Reservation execution and recovery — Phase 7

## Install and verify

Stop existing API, provider and worker processes before installing. All workers
must run this version's dispatch guard; mixed old/new worker deployments are
outside this local lab's contract.

The Phase 7 ZIP is executable by Python as an installer. From the repository root:

```bash
python /mnt/c/Users/Shax/Downloads/retry_window_lab_phase7_recovery.zip
docker compose up -d --wait --wait-timeout 120
python -m retry_window_lab.migrate
python -m retry_window_lab.migrate
python -m retry_window_lab.provision
python -m pytest -q tests/unit
python -m pytest -q tests/integration/test_recovery.py
python -m pytest -q
```

The installer verifies all replaced files against their Phase 6 hashes and
refuses to overwrite existing new files. It checks the complete archive before
writing. Its `__main__.py` stays inside the download; it is not installed into
the repository.

An existing Phase 6 database applies queue migration `[3]` once, followed by no
pending migrations. Queue migrations 1/2 and provider migration 1 are unchanged.
Provisioning adds explicit application-role access to the new queue-side tables.
No dependency change is needed.

Verified on Ubuntu / WSL2 with PostgreSQL 17 and Redis 7.4:
228 unit tests and 123 integration tests passed, for 351 total.
All 25 recovery integration cases passed. Queue migration 3 and updated
application-role grants applied successfully.

The live HTTP recovery demonstration passed all six scenarios:
normal execution, response loss after expiry, delayed visibility, proven
no-effect retry, unavailable lookup and delayed original-request settlement.
Independent operator checks observed one provider effect per operation.

## Run the recovery demonstration

Start the provider in one terminal:

```bash
python -m uvicorn retry_window_lab.provider_api:app --host 127.0.0.1 --port 8001
```

In another terminal, from the repository root with the virtual environment active:

```bash
python -m retry_window_lab.recovery_demo
```

All six named checks should be true and provider_effects_per_operation should
be 1. The demonstration includes real one-second observation intervals and can
take several seconds. It creates isolated queue storage and a unique Redis
stream, then removes those resources. It retains fresh provider experiment
records for inspection. Existing queue notifications are not redirected into
the demonstration stream.

The demo runs the Worker class and recovery service within the operator process;
the separate-process worker behavior was established in Phase 5 and remains in
the regression suite. Its provider calls use real HTTP and restricted application
credentials. Only its independent operator controls and ground-truth checks use
bootstrap/provider storage access. Queue runtime code does not read provider
storage directly.

## Configure a reservation task

Use the normal task API to submit a reserve_inventory definition. Before the
first execution, bind provider context:

POST `/tasks/{task_id}/provider-context`

```json
{
  "experiment_id": "11111111-1111-4111-8111-111111111111",
  "window_seconds": 60,
  "profile_version": 1
}
```

Use an actual experiment UUID created with provider_admin. Its profile, version
and window must agree with the task definition and binding. The provider origin
comes from RWL_PROVIDER_URL, default http://127.0.0.1:8001. Only a loopback HTTP
origin with an explicit port is accepted. The origin is persisted in the context
so changing a worker's environment cannot redirect an already bound task.

A matching bind is idempotent. Changing bound context returns HTTP 409; binding
is allowed only before the first claim. Context creation, delivery-generation
increment and a new TASK_READY outbox row commit together. This makes newly
configured work discoverable even if a worker previously left its unconfigured
notification pending. Earlier generations become obsolete. Unconfigured
reservation tasks remain QUEUED and are not executed.

GET `/tasks/{task_id}/recovery` exposes context, mutation/observation counts,
persisted due times and decision reasons. The normal task-status endpoint returns
the business result. The provider idempotency key is the stable operation UUID;
experiment UUID namespaces separate experimental runs.

## Durable execution sequence

1. Claim a PostgreSQL lease and create an execution attempt.
2. Acknowledge its Redis notification after that claim commits.
3. Under the current task lease, create a unique dispatch marker with the exact
   provider request body; commit before POST.
4. Send one HTTP request outside the database transaction while renewing the lease.
5. Under the current lease, atomically persist the observation, decision, attempt
   outcome, task result/state and any persisted retry deadline.

A dispatch marker is never reused to issue a second POST. The HTTP client disables
redirects and environment proxies and performs no automatic retries. It bounds
response reads to 262,144 bytes, uses a two-second socket timeout, rejects invalid
JSON evidence and records bounded error classifications without exception text.
The timeout is a socket-operation timeout, not a hard total wall-clock deadline.

Replies must match experiment, operation, SKU, quantity, provider fingerprint,
profile, version and window before they can establish mutation success or a
known no-effect rejection. Success also requires valid reservation identity and
acceptance/expiry times. Wrong-profile or malformed responses remain uncertain.
A response's HTTP status by itself never establishes no effect.

## Evidence policy version 1

| Evidence | Action |
| --- | --- |
| Matching successful mutation reply | SUCCEEDED with reservation evidence |
| Matching lookup with exactly one visible reservation | SUCCEEDED with lookup evidence |
| Confirmed insufficient-stock rejection | FAILED; no retry |
| Confirmed settled-no-effect response, with all prior dispatches also settled without effect | RETRY_WAIT if the execution budget remains |
| Expired lease before any dispatch marker for that attempt, with no unresolved prior dispatch | RETRY_WAIT if the execution budget remains |
| Timeout, response loss, processing reply or lease loss after a dispatch marker | RECONCILING |
| NOT_FOUND, unavailable lookup or other inconclusive observation | Keep reconciling within the observation budget |
| Duplicate visible effects or mismatched lookup evidence | NEEDS_REVIEW |
| Observation budget exhausted with uncertainty | NEEDS_REVIEW |
| Execution budget exhausted after proven no-effect work | FAILED with MUTATION_BUDGET_EXHAUSTED |

The policy never retries solely because a key is still locally believed protected,
because protection expired, or because a lookup is negative. It does not infer
settlement of every dispatch from provider receipt lists. This conservative
version supports retries only when it can establish no unresolved competing
dispatch; comparison policies can deliberately relax that rule in later phases.

An authoritative lookup reporting a matching reservation alongside a still-
processing request remains inconclusive. Delayed lookup can hide pending or
new effects; this lab assumes the operation namespace belongs to its task and
that the provider follows its declared contract. It does not claim protection
against malicious providers or uncontrolled external writers.

## Leases, budgets and scheduling

Execution and reconciliation attempts have distinct purposes, UUIDs and monotonic
lease tokens. Each reconciliation claim consumes an observation slot before
network I/O; crashing cannot reset the budget. Reconciliation results are subject
to the same database-time, current-attempt and token guards as execution results.
A stale result cannot overwrite a newer owner or a terminal task.

Defaults are at most 3 reservation execution claims and 5 reconciliation claims
per task, with one-second observation intervals. Execution claims that crash
before dispatch still consume the execution budget. Retry delay uses full jitter,
base one second and cap eight seconds; the chosen PostgreSQL due time is persisted
and restarting does not resample it. Seeded randomness can be injected in tests.

The scheduler locks due RETRY_WAIT tasks, changes them to QUEUED, increments
delivery_generation and inserts a fresh outbox row in one transaction. Concurrent
schedulers cannot create competing current generations. Dispatcher publication
and consumer-group handling retain the duplicate-tolerant contract from Phase 4/5.

Continuous workers scan expiry, schedule due retries and perform one due
reconciliation between normal notification reads. The manual recover command
performs one bounded expiry/scheduling cycle and at most one reconciliation:

```bash
python -m retry_window_lab.recover
```

Keep the dispatcher running to publish newly scheduled work. Recovery scans can
also run separately when workers are unavailable or busy. Reconciliation uses a
30-second database lease and a bounded HTTP socket timeout; late results are
rejected by the completion guard.

## Crash boundary and guarantees

If expiry wins the task lock before begin_dispatch, the old token cannot create
a marker and the worker does not POST. This permits a proven-not-dispatched retry.
If begin_dispatch committed first, the scanner must assume that the old process
could still issue its POST later. It reconciles or requires review rather than
authorizing another mutation. A database lease does not cancel an HTTP request
or fence the provider.

The tests cover both sides of this boundary, a subprocess exiting abruptly after
its marker, lease loss during reconciliation, stale completions, budgets,
concurrent scanners/schedulers, durable deadlines, context binding and atomic
outbox rollback. Most recovery integration cases use HTTP TestClient adapters
against isolated provider storage; the source-preparation smoke run also exercised
real TCP HTTP and separate worker CLI processes. Post-commit response loss uses
the simulator's deterministic 503; it is not a literal TCP-disconnection test.

No exactly-once external execution, Redis data-loss reconstruction, production
network-partition fault tolerance or throughput result is claimed. Further work
includes notification reconstruction, capacity/backpressure, observability and
controlled comparisons of recovery policies.
