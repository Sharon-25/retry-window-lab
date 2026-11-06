# Local execution — Phase 3

## Run

From the project root with the virtual environment active:

```bash
python -m retry_window_lab.demo
python -m pytest -q tests/unit
python -m pytest -q
```

The unit suite contains 77 cases. Together with the two Phase 2 integration tests, the expected full-suite count is 79. Integration tests require the PostgreSQL and Redis containers to be healthy.

## Files

- src/retry_window_lab/domain.py: validated definitions, task kinds, lifecycle rules, task and attempt snapshots.
- src/retry_window_lab/local.py: synchronous handler registry and serial in-memory execution.
- src/retry_window_lab/demo.py: executable checksum demonstration and repeated submission.
- tests/unit/test_local_execution.py: input, byte-boundary, identity, state-transition and outcome tests.

## Input examples

Checksum, implemented locally:

```python
TaskDefinition("checksum", "checksum-001", {"text": "abc"})
```

Inventory reservation, validated but not executable until a handler is implemented:

```python
TaskDefinition("reserve_inventory", "order-001", {
    "sku": "SKU-BOOK-001",
    "quantity": 2,
    "provider_profile": "authoritative",
})
```

Known SKUs: SKU-BOOK-001 and SKU-PEN-001. Provider profiles: authoritative, delayed and unavailable. This phase does not maintain inventory stock or contact a provider.

## Definition and identity rules

The definition validates exact payload fields before creating execution records. Submission keys are nonblank strings with at most 128 characters. Quantities must be positive integers; booleans are rejected. Unknown kinds, SKUs, profiles and extra payload fields are rejected.

The 65,536-byte limit applies to the compact UTF-8 JSON payload, including JSON syntax and escaping, not only to the text field's character count. Empty checksum text is valid. Invalid UTF-8 content is rejected.

The definition stores a serialized payload snapshot. Reading payload returns a fresh object, so mutation of the original input or returned dictionary does not change the accepted definition.

The SHA-256 fingerprint includes schema version 1, task kind and sorted compact JSON payload; it excludes the submission key and generated identifiers. Different key order gives the same fingerprint. Different business data changes the fingerprint. This is the project's restricted Python JSON encoding contract, not a claim of a cross-language canonical JSON standard or an authentication mechanism.

The local executor maps submission_key to task record. Matching re-submission returns the original task, operation, attempt and outcome without running the handler again. A changed fingerprint under the same key raises SubmissionConflict. A new key creates a separate task, even for identical work. Durable multi-process submission arbitration belongs in Phase 4.

## Lifecycle and outcomes

The transition validator implements the Phase 1 state table. Same-state transitions are rejected. SUCCEEDED and FAILED have no outgoing transitions. NEEDS_REVIEW has no automated outgoing transition in this release.

The local executor currently exercises QUEUED -> RUNNING -> SUCCEEDED / FAILED / NEEDS_REVIEW. RETRY_WAIT and RECONCILING are defined and transition-tested; their scheduling and recovery mechanisms are not implemented yet.

Each execution records distinct task, operation and attempt UUIDs. Attempts include worker identity, attempt number, timezone-aware UTC timestamps, outcome and bounded error classification. These timestamps are event records, not performance-duration measurements.

- A valid JSON-object result records SUCCEEDED.
- KnownTaskFailure is an explicit handler contract: permanent rejection with no external effect. It records FAILED and KNOWN_FAILURE.
- Unexpected ordinary exceptions record NEEDS_REVIEW and UNKNOWN. This conservative behavior does not assert that an external effect failed.
- An invalid or unserializable handler result also records UNKNOWN; the handler could already have applied an effect.
- Exception messages are not stored in task error records.
- KeyboardInterrupt and other BaseException subclasses propagate. An interrupted local attempt remains RUNNING in memory. Durable interruption recovery belongs in Phase 5.

Handlers in this phase are synchronous callables returning JSON dictionaries. The registry rejects duplicate registration and missing handlers. It is not an arbitrary-code upload interface.

## Expected demonstration

Input text: abc.

- state: SUCCEEDED
- attempt_count: 1
- attempt_outcome: SUCCEEDED
- input_bytes: 3
- sha256: ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
- resubmission_reused_task: true

UUIDs vary on each demonstration process. Local state is lost when the process exits. No exactly-once external execution, durability, parallel safety or crash recovery is claimed by this exercise.

## Verification scope

Verified on Ubuntu / WSL2: 77 unit tests passed; the complete suite passed all 79 tests, including PostgreSQL and Redis integration tests. The checksum demonstration produced the expected digest and reused the original task on matching resubmission.

No new third-party dependency is introduced in this phase.
