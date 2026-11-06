from dataclasses import FrozenInstanceError
from itertools import product

import pytest

from retry_window_lab.domain import (
    MAX_PAYLOAD_BYTES, AttemptOutcome, TaskDefinition, TaskKind,
    TaskState, canonical_json, validate_transition,
)
from retry_window_lab.local import (
    HandlerRegistry, KnownTaskFailure, LocalExecutor,
    SubmissionConflict, checksum_handler,
)


def definition(text="abc", key="test-key"):
    return TaskDefinition("checksum", key, {"text": text})


def executor_for(handler=checksum_handler):
    registry = HandlerRegistry()
    registry.register(TaskKind.CHECKSUM, handler)
    return LocalExecutor(registry)


@pytest.mark.parametrize("quantity", [True, False, 0, -1, 1.5, "2", None])
def test_invalid_reservation_quantities(quantity):
    with pytest.raises(ValueError):
        TaskDefinition("reserve_inventory", "reserve-1", {
            "sku": "SKU-BOOK-001", "quantity": quantity,
            "provider_profile": "authoritative",
        })


@pytest.mark.parametrize("kind,key,payload", [
    ("missing", "key", {"text": "abc"}),
    ("checksum", " ", {"text": "abc"}),
    ("checksum", "k" * 129, {"text": "abc"}),
    ("checksum", "key", {"text": 5}),
    ("checksum", "key", {"text": "abc", "extra": 1}),
    ("checksum", "key", {}),
    ("checksum", "key", []),
    ("checksum", "key", {"text": "\ud800"}),
    ("reserve_inventory", "key", {
        "sku": "UNKNOWN", "quantity": 1, "provider_profile": "authoritative",
    }),
    ("reserve_inventory", "key", {
        "sku": "SKU-PEN-001", "quantity": 1, "provider_profile": "missing",
    }),
])
def test_invalid_submissions(kind, key, payload):
    with pytest.raises(ValueError):
        TaskDefinition(kind, key, payload)


def test_payload_size_boundary():
    overhead = len(canonical_json({"text": ""}).encode("utf-8"))
    text = "a" * (MAX_PAYLOAD_BYTES - overhead)
    definition(text)
    with pytest.raises(ValueError):
        definition(text + "a")
    with pytest.raises(ValueError):
        definition("é" * (MAX_PAYLOAD_BYTES // 2))


def test_payload_snapshot_and_fingerprint():
    payload = {"sku": "SKU-BOOK-001", "quantity": 2, "provider_profile": "delayed"}
    first = TaskDefinition("reserve_inventory", "one", payload)
    second = TaskDefinition("reserve_inventory", "two", dict(reversed(list(payload.items()))))
    assert first.fingerprint == second.fingerprint
    payload["quantity"] = 99
    copy = first.payload
    copy["quantity"] = 77
    assert first.payload["quantity"] == 2
    with pytest.raises(FrozenInstanceError):
        first.submission_key = "changed"
    assert definition("a").fingerprint != definition("b").fingerprint


ALLOWED_EDGES = {
    ("QUEUED", "RUNNING"),
    ("RUNNING", "SUCCEEDED"), ("RUNNING", "FAILED"),
    ("RUNNING", "RETRY_WAIT"), ("RUNNING", "RECONCILING"),
    ("RUNNING", "NEEDS_REVIEW"), ("RETRY_WAIT", "QUEUED"),
    ("RECONCILING", "SUCCEEDED"), ("RECONCILING", "FAILED"),
    ("RECONCILING", "RETRY_WAIT"), ("RECONCILING", "NEEDS_REVIEW"),
}


@pytest.mark.parametrize("current,target", list(product(TaskState, repeat=2)))
def test_state_transition_contract(current, target):
    if (current.value, target.value) in ALLOWED_EDGES:
        validate_transition(current, target)
    else:
        with pytest.raises(ValueError):
            validate_transition(current, target)


def test_checksum_success_and_attempt():
    task = executor_for().submit_and_run(definition())
    assert task.state == TaskState.SUCCEEDED
    assert task.result == {
        "sha256": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        "input_bytes": 3,
    }
    attempt = task.attempts[0]
    assert attempt.number == 1
    assert attempt.outcome == AttemptOutcome.SUCCEEDED
    assert attempt.finished_at >= attempt.started_at
    assert attempt.started_at.utcoffset().total_seconds() == 0
    assert len({task.task_id, task.operation_id, attempt.attempt_id}) == 3
    result = task.result
    result["input_bytes"] = 999
    assert task.result["input_bytes"] == 3


def test_duplicate_submission_does_not_execute_again():
    calls = []

    def handler(payload):
        calls.append(payload)
        return checksum_handler(payload)

    executor = executor_for(handler)
    first = executor.submit_and_run(definition())
    assert executor.submit_and_run(definition()) == first
    with pytest.raises(SubmissionConflict):
        executor.submit_and_run(definition("different"))
    assert executor.get("test-key") == first
    assert len(calls) == 1


def test_known_failure():
    def handler(payload):
        raise KnownTaskFailure("permanent rejection; no effect")

    task = executor_for(handler).submit_and_run(definition())
    assert task.state == TaskState.FAILED
    assert task.attempts[0].outcome == AttemptOutcome.KNOWN_FAILURE
    assert task.result is None


def test_exception_after_effect_is_unknown_and_not_retried():
    effects = []

    def handler(payload):
        effects.append("effect-applied")
        raise TimeoutError("response lost")

    executor = executor_for(handler)
    task = executor.submit_and_run(definition())
    assert task.state == TaskState.NEEDS_REVIEW
    assert task.attempts[0].outcome == AttemptOutcome.UNKNOWN
    assert task.result is None
    assert executor.submit_and_run(definition()) == task
    assert len(effects) == 1


@pytest.mark.parametrize("result", [None, {"value": float("nan")}, {"value": object()}])
def test_invalid_handler_result_is_unknown(result):
    task = executor_for(lambda payload: result).submit_and_run(definition())
    assert task.state == TaskState.NEEDS_REVIEW
    assert task.result is None


def test_registry_rejects_missing_and_duplicate_handlers():
    registry = HandlerRegistry()
    executor = LocalExecutor(registry)
    with pytest.raises(ValueError):
        executor.submit_and_run(definition())
    with pytest.raises(KeyError):
        executor.get("test-key")
    registry.register(TaskKind.CHECKSUM, checksum_handler)
    with pytest.raises(ValueError):
        registry.register(TaskKind.CHECKSUM, checksum_handler)


def test_interrupt_is_not_reported_as_known_failure():
    def handler(payload):
        raise KeyboardInterrupt()

    executor = executor_for(handler)
    with pytest.raises(KeyboardInterrupt):
        executor.submit_and_run(definition())
    assert executor.get("test-key").state == TaskState.RUNNING
