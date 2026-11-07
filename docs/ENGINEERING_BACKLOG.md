# Engineering Backlog

## Phase 1 — Contract and architecture

- Complete: system contract and architecture specification.

## Phase 2 — Environment and repository

- Verified: Ubuntu / WSL2 and Docker container execution.
- Verified: project virtual environment and dependency installation.
- Verified: dependency consistency check.
- Verified: PostgreSQL and Redis container health checks.
- Verified: PostgreSQL transaction integration test.
- Verified: Redis Streams delivery and acknowledgement integration test.
- Result: 2 integration tests passed.
- Complete: initial commit pushed to origin/master; synchronization verified.

## Remaining engineering work

- Restricted database roles and provider-data isolation.
- Transactional outbox and Redis notification reconstruction.
- Worker leases and stale-attempt rejection.
- Recovery decisions across provider protection expiry.
- Controlled failure experiments and policy comparisons.

## Phase 3 — Task model and local execution

- Verified: payload validation and stable fingerprints.
- Verified: lifecycle transition rules and immutable payload snapshots.
- Verified: task, operation and attempt identities.
- Verified: handler registration and local checksum execution.
- Verified: matching resubmission reuse and conflicting submission rejection.
- Verified: known failure and uncertain-outcome classification.
- Verified: 77 unit tests and 79 total tests passed on Ubuntu / WSL2.
- Verified: checksum demonstration and unchanged dependency requirements.
- Implementation and verification complete; Git synchronization is checked
  after pushing the milestone commit.

## Phase 4 — Durable submission and delivery

- Verified: versioned migrations and migration consistency checks.
- Verified: durable submissions and atomic submission-key conflict handling.
- Verified: task acceptance and outbox creation in one transaction.
- Verified: Redis Streams publication and conditional publication marking.
- Verified: submission and task-status API endpoints.
- Verified: concurrent submissions, rollback and publication failure scenarios.
- Verified: 131 tests passed on Ubuntu / WSL2.
- Verified: manual API submission, reuse, retrieval and dispatcher demonstration.
- Implementation and verification complete; remote synchronization is checked
  after pushing the milestone commit.

## Compatibility follow-up

- Track the upstream TestClient/httpx deprecation warning.
  All current tests pass; no compatibility change is required for this milestone.

## Phase 5 — Distributed workers and ownership

- Verified: migration 2 preserves existing tasks and publication intent.
- Verified: consumer groups and pending-notification reclamation.
- Verified: atomic claims, durable attempts and monotonic ownership tokens.
- Verified: checksum execution and transactional result persistence.
- Verified: heartbeat renewal and stale completion rejection.
- Verified: expired attempts become UNKNOWN and tasks require review.
- Verified: concurrent processes, duplicate notifications and abrupt process loss.
- Verified: 201 tests passed on Ubuntu / WSL2.
- Verified: manual demonstration completed eight tasks with eight attempts.
- Implementation and verification complete; remote synchronization is checked
  after pushing the milestone commit.

## Phase 6 — Provider simulator and isolation

- Verified: separate provider schema and versioned migration ledger.
- Verified: restricted application/provider roles and cross-schema denial.
- Verified: atomic reservations, stock consistency and protected-key conflicts.
- Verified: finite protection expiry with retained reservation history.
- Verified: authoritative, delayed and unavailable lookup profiles.
- Verified: controlled provider time and one-shot failure injection.
- Verified: post-commit response loss and delayed original-request commitment.
- Verified: 273 tests passed on Ubuntu / WSL2.
- Verified: live HTTP demonstration passed all six behavior checks.
- Implementation and verification complete; remote synchronization is checked
  after pushing the milestone commit.

## Phase 7 — Reservation execution and recovery

- Verified: migration 3 and restricted-role grants for recovery storage.
- Verified: immutable provider context and atomic readiness publication.
- Verified: guarded dispatch records and reservation worker execution.
- Verified: persisted provider observations and versioned decisions.
- Verified: reconciliation after uncertain outcomes.
- Verified: retries require proven no-effect evidence.
- Verified: execution/observation budgets and durable retry deadlines.
- Verified: concurrent scanners, scheduler rollback and stale-result rejection.
- Verified: 351 tests passed on Ubuntu / WSL2.
- Verified: six HTTP recovery scenarios with one observed effect per operation.
- Implementation and verification complete; remote synchronization is checked
  after pushing the milestone commit.

## Phase 8 — Concurrency, capacity and notification recovery

- Verified atomic queue admission, HTTP 429 at capacity and idempotent replay.
- Verified worker slot limits and recovery progress with occupied execution slots.
- Verified Redis notification reconstruction, duplicate replay and worker interruption.
- Migration 4 and restricted application grants applied successfully.
- All eight Phase 8 integration tests and all 359 project tests passed on Ubuntu / WSL2.

## Phase 9 — Observability and monitoring

- Verified queue snapshots, active leases and recovery metrics.
- Verified the dashboard and correlated task history in the browser.
- Verified that committed recovery decisions match structured log IDs.
- All 364 tests passed on Ubuntu / WSL2.
