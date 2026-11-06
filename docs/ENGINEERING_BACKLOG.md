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
