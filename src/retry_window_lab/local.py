import hashlib
from dataclasses import replace
from typing import Callable

from retry_window_lab.domain import (
    AttemptOutcome, AttemptRecord, TaskDefinition, TaskKind,
    TaskRecord, TaskState, canonical_json, utc_now, validate_transition,
)


Handler = Callable[[dict], dict]


class SubmissionConflict(ValueError):
    pass


class KnownTaskFailure(Exception):
    """Handler explicitly guarantees a permanent failure with no effect."""


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[TaskKind, Handler] = {}

    def register(self, kind: TaskKind, handler: Handler) -> None:
        if not isinstance(kind, TaskKind) or not callable(handler):
            raise ValueError("Registration requires a TaskKind and callable")
        if kind in self._handlers:
            raise ValueError(f"Handler already registered: {kind}")
        self._handlers[kind] = handler

    def resolve(self, kind: TaskKind) -> Handler:
        if kind not in self._handlers:
            raise ValueError(f"No handler registered: {kind}")
        return self._handlers[kind]


def checksum_handler(payload: dict) -> dict:
    raw = payload["text"].encode("utf-8")
    return {"sha256": hashlib.sha256(raw).hexdigest(), "input_bytes": len(raw)}


class LocalExecutor:
    """Serial, in-memory exercise; not durable or safe for concurrent submission."""

    def __init__(self, registry: HandlerRegistry, worker_id: str = "local-worker"):
        if not isinstance(worker_id, str) or not worker_id.strip():
            raise ValueError("worker_id must be nonempty")
        self._registry = registry
        self._worker_id = worker_id
        self._tasks: dict[str, TaskRecord] = {}

    def submit_and_run(self, definition: TaskDefinition) -> TaskRecord:
        key = definition.submission_key
        existing = self._tasks.get(key)
        if existing is not None:
            if existing.definition.fingerprint != definition.fingerprint:
                raise SubmissionConflict("submission_key reused with different work")
            return existing

        handler = self._registry.resolve(definition.kind)
        task = TaskRecord(definition=definition)
        attempt = AttemptRecord(number=1, worker_id=self._worker_id)
        validate_transition(task.state, TaskState.RUNNING)
        task = replace(task, state=TaskState.RUNNING, attempts=(attempt,))
        self._tasks[key] = task
        result_json = None

        try:
            result = handler(definition.payload)
            if type(result) is not dict:
                raise TypeError("Handler must return a JSON object")
            result_json = canonical_json(result)
            result_json.encode("utf-8")
        except KnownTaskFailure:
            state = TaskState.FAILED
            outcome = AttemptOutcome.KNOWN_FAILURE
            error_code = "KNOWN_PERMANENT_FAILURE"
        except Exception:
            state = TaskState.NEEDS_REVIEW
            outcome = AttemptOutcome.UNKNOWN
            error_code = "UNEXPECTED_HANDLER_ERROR"
            result_json = None
        else:
            state = TaskState.SUCCEEDED
            outcome = AttemptOutcome.SUCCEEDED
            error_code = None

        validate_transition(task.state, state)
        finished = replace(
            attempt, finished_at=utc_now(), outcome=outcome, error_code=error_code,
        )
        task = replace(task, state=state, attempts=(finished,), result_json=result_json)
        self._tasks[key] = task
        return task

    def get(self, submission_key: str) -> TaskRecord:
        return self._tasks[submission_key]
