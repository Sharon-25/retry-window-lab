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
- Pending: initial commit, GitHub push and remote synchronization.

## Remaining engineering work

- Restricted database roles and provider-data isolation.
- Transactional outbox and Redis notification reconstruction.
- Worker leases and stale-attempt rejection.
- Recovery decisions across provider protection expiry.
- Controlled failure experiments and policy comparisons.
