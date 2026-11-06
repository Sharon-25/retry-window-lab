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
