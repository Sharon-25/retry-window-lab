# Retry Window Lab — Phase 1 Specification

Version: 1.0 | Design baseline: 6 November 2026

Status: specification complete; implementation and runtime verification pending.

## 1. Purpose and scope

Build a Python distributed task queue that evaluates recovery when an external provider's idempotency protection expires. Compare blind retry, stable-key retry and evidence-based recovery against controlled provider failures.

Originality statement: a distinctive implementation and experimental focus; not a claim that these distributed-systems techniques are new or that no similar project exists.

First release: one queue, one operation (inventory reservation), multiple worker processes, bounded concurrency, durable task/attempt history, retries, recovery, backpressure, monitoring and comparative reports. A checksum handler provides the earlier local-execution exercise.

Exclude real payments, arbitrary uploaded code, automatic scaling, workflow DAGs, Kafka, a frontend dashboard and multi-region availability. Provider time travel is restricted to isolated experiments.

## 2. Concrete operation and identities

Example request body:

```json
{
  "submission_key": "demo-order-001",
  "provider_profile": "authoritative",
  "sku": "SKU-BOOK-001",
  "quantity": 2
}
```

Initial simulator stock: SKU-BOOK-001 = 100 units; SKU-PEN-001 = 100 units. Each experiment starts with fresh, isolated stock and provider records.

- task_id: UUID for the accepted task; stable across attempts.
- attempt_id: UUID for one execution or reconciliation attempt.
- operation_id: stable identity of the intended external reservation.
- submission_key: client identity for submission deduplication.
- delivery_generation: monotonic task field distinguishing successive schedules.
- lease_token: monotonic task ownership generation used in conditional updates.
- experiment_id and policy_id: separate otherwise equivalent experimental runs.

The same submission_key and canonical payload return the existing task. Reusing it with different business parameters returns HTTP 409. Experiments namespace all identities, including provider idempotency keys, so one policy cannot benefit from another policy's work.

Validation: known SKU and profile; integer quantity > 0 (booleans rejected); configured payload-size limit. Malformed requests are rejected before acceptance. Insufficient stock is a permanent business rejection, not a transient infrastructure failure.

Business success means exactly one matching reservation is confirmed for the operation. Worker exit status alone does not establish business success. More than one reservation is a duplicate effect, even if the task record shows one success.

## 3. Provider contracts

All three simulator profiles have a finite, configurable idempotency window. Initial experiment default: 60 provider-seconds. Keys are scoped to provider namespace + operation and bound to a request fingerprint. Concurrent same-key requests within protection are atomically deduplicated. Reuse with a conflicting payload returns 409.

Protection starts when the provider first accepts the mutation, not when the task is submitted. Expired keys can be forgotten while reservation records remain. A key reused after expiry can create another reservation.

| Profile | Lookup contract | Recovery implication |
| --- | --- | --- |
| authoritative | Lookup can confirm a committed reservation immediately; a status endpoint can also expose processing or settled-with-no-effect | Confirm success from matching evidence. Absence alone does not authorize a new mutation. |
| delayed | Committed reservations appear after a configured visibility delay | A negative lookup is inconclusive; wait/reconcile or require review. |
| unavailable | No reservation lookup | Only a still-applicable provider guarantee or other conclusive evidence can justify retry; otherwise require review. |

Critical rule: even authoritative NOT_FOUND does not prove that a previously dispatched request cannot arrive or commit later. Re-dispatch after protection expiry requires proof that the old request is settled with no effect, or an atomic provider mechanism preventing duplicate commitment. Without that proof, NEEDS_REVIEW is correct.

Record provider-profile version, contract duration, first-dispatch time, any confirmed provider acceptance/expiry timestamps and observations. A local cutoff is a conservative estimate, not proof that a delayed request will reach the provider before expiry. Clock uncertainty, transit delay and concurrent old attempts must be reflected in the decision. Do not claim exactly-once external execution.

## 4. Lifecycle

| State | Meaning | Next states |
| --- | --- | --- |
| QUEUED | Accepted and eligible for a scheduled attempt | RUNNING |
| RUNNING | Worker holds a current database lease | SUCCEEDED, FAILED, RETRY_WAIT, RECONCILING, NEEDS_REVIEW |
| RETRY_WAIT | Waiting for a justified retry or observation | QUEUED |
| RECONCILING | Evaluating provider evidence without blindly repeating a mutation | SUCCEEDED, FAILED, RETRY_WAIT, NEEDS_REVIEW |
| NEEDS_REVIEW | Automated progress stopped because evidence is insufficient | No automated mutation in version one |
| SUCCEEDED | Matching external success confirmed | Terminal |
| FAILED | Permanent rejection or known unsuccessful work with no further attempts allowed | Terminal |

NEEDS_REVIEW is operationally unresolved, not success or proof of failure. Version one exposes evidence for inspection; manual force-replay is deferred.

A lease-expiration scanner records the interrupted attempt as having an unknown outcome and routes the task to recovery. It never interprets lease expiry as proof that no external effect occurred. Reconciliation has its own lease and bounded observations. Status and scheduling changes are committed atomically.

## 5. Recovery policy

Production-path policy: evidence-based recovery. Deliberately unsafe baselines run only against isolated simulator namespaces.

| Available evidence | Action |
| --- | --- |
| Matching committed reservation confirmed | Record SUCCEEDED; do not mutate again |
| Provider conclusively rejected the operation with no effect | FAILED for permanent rejection; retry only for eligible transient rejection |
| Local validation failed before dispatch | Reject submission or record a known non-dispatched failure |
| Provider confirms original operation settled with no effect | Retry if budget remains and no unresolved competing dispatch exists |
| Outcome unknown; provider protection is demonstrably applicable to this retry | Same-key retry may be eligible under the explicit simulator contract |
| Protection expired or applicability cannot be established; lookup available | Reconcile; negative or stale observations do not by themselves permit retry |
| Outcome unknown; no conclusive evidence within observation budget | NEEDS_REVIEW |

Every decision stores a reason code, relevant timestamps, profile version, observation references and policy version. Reason codes include EFFECT_CONFIRMED, PERMANENT_REJECTION, SETTLED_NO_EFFECT, PROTECTED_RETRY, LOOKUP_INCONCLUSIVE, PROTECTION_EXPIRED and EVIDENCE_EXHAUSTED.

Initial operational defaults (configurable and not performance claims): maximum 3 mutation attempts per task; maximum 5 reconciliation observations; exponential retry delay with full jitter, base 1 second and cap 8 seconds. Exhaustion with an uncertain external outcome goes to NEEDS_REVIEW, not FAILED.

Persist chosen retry due-times so restarting does not reset the delay. Use seeded policy randomness in reproducible experiments, without claiming deterministic OS/network scheduling.

## 6. Architecture and responsibilities

Python implements the API, dispatcher, recovery scanner, workers, provider simulator and experiment runner. These are logical modules with a small number of process entry points, not separate microservices for each module.

PostgreSQL is authoritative for acceptance, task state, attempts, leases, results, decisions and publication intent. Redis Streams delivers ready-work notifications. Redis does not decide whether a task owns a current lease or whether an external retry is safe.

Submission flow:
1. Validate input and deduplicate submission.
2. In one PostgreSQL transaction, create the task and its outbox event.
3. Commit; only then return HTTP 202 with task_id.
4. A dispatcher publishes the outbox notification to Redis and marks publication afterward.

Outbox publication can repeat if the dispatcher crashes between publish and mark. Workers tolerate duplicate notifications using task state and delivery_generation checks in PostgreSQL.

Worker flow:
1. Read a notification from the Redis consumer group.
2. Atomically verify eligibility and claim a database lease, create an attempt and update task state.
3. Acknowledge the notification after durable claim, or after verifying it is obsolete/already represented by durable state.
4. Execute outside the database transaction; renew the database lease while active.
5. Conditionally commit outcome and any follow-up schedule using current lease_token and state.

The database lease is the sole execution-ownership authority. A Redis acknowledgement means the notification has been durably handled; it does not mean the business task succeeded. Pending notifications from crashed consumers are reclaimed and revalidated against PostgreSQL. Redis idle time is not an execution lease.

On Redis loss, rebuild notifications for eligible durable tasks and due schedules. On worker loss after notification acknowledgement, recover through the database lease scanner. PostgreSQL unavailability stops acceptance and new claims; an already-sent provider request may still complete, so its later recovery must preserve uncertainty.

All scheduled transitions, including retries, use database transactions and outbox records. Use database time for leases and conditional state updates. Never hold row locks across network calls. Redis Streams alone supplies neither deadline scheduling nor safe external effects.

Initial PostgreSQL table responsibilities:
- tasks: current state, operation identity, payload fingerprint, delivery generation, lease fields, scheduling and counters.
- attempts: append-preserved execution/observation history with interruption and outcome records.
- outbox: durable delivery intent and publication progress.
- recovery_decisions: decision reasons and evidence references.
- experiment_runs: policy, scenario, seed and environment metadata.

The provider uses a separate database namespace and connection identity for reservations, request processing and idempotency records. Workers cannot read its hidden ground truth. The evaluator reads that truth through a separate test-only interface.

## 7. Concurrency, distribution and overload

Workers pull notifications only when they have execution slots. Begin with two independent workers, each with concurrency 2. This is demand-driven distribution, not a claim of perfectly equal utilization.

Use asyncio for network/database waits; keep CPU-heavy handlers out of the event loop. The early checksum handler uses bounded input. Detailed process-pool support is deferred unless a measured workload requires it.

At most one current database attempt may finalize a task. Stale lease tokens cannot update authoritative state. This does not stop stale workers from making external calls; provider-side contracts and conservative recovery must address that boundary.

Start with a configurable outstanding-task limit of 1000 (all nonterminal states, including NEEDS_REVIEW). Admission arbitration must be atomic across API instances. Return 429 for capacity rejection and 503 for unavailable required infrastructure; rejected submissions are not accepted tasks. Idempotent resubmission can still return an existing task at capacity.

Reserve recovery capacity through a bounded scheduling policy so repeated failures do not monopolize slots. The exact selection algorithm belongs in Phase 8 and will be tested with uneven task durations.

## 8. Required experiment cases and expected evidence-based outcomes

| ID | Scenario | Expected outcome |
| --- | --- | --- |
| E01 | Normal reservation and response | SUCCEEDED; one effect |
| E02 | Reservation committed, response lost, authoritative lookup | SUCCEEDED after confirmation; one effect |
| E03 | Same loss, protection expired, committed reservation visible | Confirm existing reservation; no new mutation |
| E04 | Protection expired, old effect temporarily invisible | Wait/reconcile; never treat absence as permission; succeed when visible or require review |
| E05 | Protection expired, no lookup, outcome unknown | NEEDS_REVIEW |
| E06 | Duplicate Redis notification or dispatcher republish | No duplicate logical task or concurrent current claim |
| E07 | Worker killed after claim; later stale completion arrives | Recover attempt; reject stale update; evaluate effects separately |
| E08 | Redis cleared after durable acceptance | Rebuild delivery; task remains accounted for |
| E09 | Same submission key with changed quantity | HTTP 409; original task unchanged |
| E10 | Provider settles original request with no effect | Retry within budget; no duplicate effect |
| E11 | Retry sent near expiry but delayed beyond it | Do not claim local cutoff ensured safety; demonstrate boundary and preserve uncertainty where guarantees fail |
| E12 | Insufficient stock | FAILED with permanent rejection; no futile retries |

Policy comparisons use the same initial stock, request set, provider profile, random seeds and causally specified fault points. Effects and expiry events gate scenario progression; random sleeps alone do not establish equivalent failures. Record actual timing and variation in process-level runs.

Simulated provider time controls key expiry and visibility. PostgreSQL lease time remains real database time. Process-crash tests and throughput measurements use real elapsed time. Report virtual-time experiments and runtime benchmarks separately.

Metrics: duplicate extra reservations per operation, confirmed completion rate, unresolved count, known permanent failures, unnecessary holds (judged against evaluator evidence), mutation calls, observation calls and recovery latency. An honest unresolved outcome is distinct from a failed recovery that caused a duplicate. Zero observed duplicates is scoped to tested scenarios, not a universal guarantee.

## 9. Guarantees and limits

- Acceptance is durable only after the configured PostgreSQL commit succeeds; assume durable storage is retained and default durable commit settings are enabled.
- Notifications and handler execution can repeat. The project does not guarantee exactly-once execution.
- A permanent provider rejection may legitimately prevent completion.
- Successful stale-update rejection does not prove absence of duplicate external effects.
- Recovery depends on available provider evidence; unsupported certainty becomes NEEDS_REVIEW.
- No protection against destruction of PostgreSQL storage, Byzantine providers or unmodelled production failures is claimed.
- A local simulator demonstrates behavior under declared contracts, not certification of a real provider integration.

## 10. Implementation phases

1. Contract and architecture — this document and review checklist.
2. Environment and repository — reproducible setup and infrastructure checks.
3. Task model and local execution — validation, transitions and handler tests.
4. Durable submission and delivery — database transaction, outbox, Redis and retrieval API.
5. Distributed workers — claims, leases, renewal and stale-update tests.
6. Provider simulator — finite idempotency, lookup profiles and controlled failures.
7. Recovery engine — decision matrix and expiry-boundary tests.
8. Concurrency and overload — bounded execution, admission and retry fairness.
9. Explanations and monitoring — correlated history and operational metrics.
10. Comparison laboratory — equivalent fault scenarios and policy reports.
11. Performance — reproducible baseline, profiling and justified optimization.
12. Release — reproducible demo, documentation, limitations and final verification.

Required correctness checks must pass before marking a phase complete. Defer optional presentation work first if scope pressure appears.

## 11. Phase 1 review checklist

- [x] Concrete operation and initial data defined.
- [x] Business success distinguished from technical completion.
- [x] Provider profiles and expiry semantics defined.
- [x] Ambiguous outcomes and not-found limitations addressed.
- [x] Task lifecycle and evidence-based decision rules defined.
- [x] PostgreSQL authority and Redis notification semantics defined.
- [x] Outbox, duplicate notification and Redis rebuild paths specified.
- [x] Clock separation, experiments and comparison metrics defined.
- [x] Scope and implementation phases recorded.
- [ ] Runtime behavior verified — deferred to implementation phases.

Design review walkthrough: E02 confirms an already-committed effect; E04 never interprets delayed absence as proof; E05 stops without asserting failure; E06 revalidates a duplicate notification; E08 rebuilds from durable state; E11 exposes the difference between local timing and provider acceptance. These are specification reviews, not executed tests.

## 12. Phase 2 subphases

2.1 Verify Windows tooling: editor terminal, Python, Git, Docker Desktop and WSL2 availability.
2.2 Create retry-window-lab project folder and Git repository; add this specification under docs/.
2.3 Create virtual environment and select/pin compatible runtime/dependency versions.
2.4 Configure PostgreSQL and Redis with Docker Compose, health checks and named storage.
2.5 Add application configuration, .env.example and .gitignore; exclude real secrets.
2.6 Create package layout and pytest configuration; add database/Redis connectivity smoke checks.
2.7 Run checks and document exact setup commands and expected output.
2.8 Commit, push to the selected GitHub remote and verify synchronization. Do not claim remote sync until an actual remote is configured and checked.

## 13. Primary references

- Redis consumer-group and acknowledgement behavior: https://redis.io/docs/latest/commands/xreadgroup/
- PostgreSQL row-lock behavior: https://www.postgresql.org/docs/current/explicit-locking.html
- Check provider key expiration and late retry guidance: https://docs.checkhq.com/docs/idempotent-requests
- Stripe API v1 pruning semantics: https://docs.stripe.com/api/idempotent_requests

These references support component behavior and the motivating problem. The architecture and numeric defaults above are project design decisions, not claims made by those sources.
