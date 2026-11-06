import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from uuid import UUID, uuid4


MAX_PAYLOAD_BYTES = 65_536
KNOWN_SKUS = frozenset({"SKU-BOOK-001", "SKU-PEN-001"})
PROVIDER_PROFILES = frozenset({"authoritative", "delayed", "unavailable"})


class TaskKind(StrEnum):
    CHECKSUM = "checksum"
    RESERVE_INVENTORY = "reserve_inventory"


class TaskState(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    RECONCILING = "RECONCILING"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class AttemptOutcome(StrEnum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    KNOWN_FAILURE = "KNOWN_FAILURE"
    UNKNOWN = "UNKNOWN"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def canonical_json(value: object) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    )


@dataclass(frozen=True, slots=True, init=False)
class TaskDefinition:
    kind: TaskKind
    submission_key: str
    _payload_json: str = field(repr=False)
    fingerprint: str

    def __init__(self, kind: str, submission_key: str, payload: dict):
        try:
            task_kind = TaskKind(kind)
        except (ValueError, TypeError):
            raise ValueError("Unsupported task kind") from None
        if (not isinstance(submission_key, str)
                or not submission_key.strip()
                or len(submission_key) > 128):
            raise ValueError("submission_key must contain 1 to 128 characters")
        if type(payload) is not dict:
            raise ValueError("payload must be a dictionary")

        if task_kind == TaskKind.CHECKSUM:
            if set(payload) != {"text"} or not isinstance(payload["text"], str):
                raise ValueError("checksum requires exactly one string field: text")
        else:
            if set(payload) != {"sku", "quantity", "provider_profile"}:
                raise ValueError("reservation requires sku, quantity and provider_profile")
            if (not isinstance(payload["sku"], str)
                    or payload["sku"] not in KNOWN_SKUS):
                raise ValueError("Unknown SKU")
            if type(payload["quantity"]) is not int or payload["quantity"] <= 0:
                raise ValueError("quantity must be a positive integer; bool is rejected")
            if (not isinstance(payload["provider_profile"], str)
                    or payload["provider_profile"] not in PROVIDER_PROFILES):
                raise ValueError("Unknown provider profile")

        try:
            encoded = canonical_json(payload)
            raw = encoded.encode("utf-8")
        except (ValueError, TypeError, UnicodeError):
            raise ValueError("payload must have a valid UTF-8 JSON representation") from None
        if len(raw) > MAX_PAYLOAD_BYTES:
            raise ValueError("payload exceeds 65536 encoded bytes")
        digest_input = canonical_json({
            "schema_version": 1, "kind": task_kind.value, "payload": payload,
        }).encode("utf-8")
        object.__setattr__(self, "kind", task_kind)
        object.__setattr__(self, "submission_key", submission_key)
        object.__setattr__(self, "_payload_json", encoded)
        object.__setattr__(self, "fingerprint", hashlib.sha256(digest_input).hexdigest())

    @property
    def payload(self) -> dict:
        return json.loads(self._payload_json)


_TRANSITIONS = {
    TaskState.QUEUED: frozenset({TaskState.RUNNING}),
    TaskState.RUNNING: frozenset({
        TaskState.SUCCEEDED, TaskState.FAILED, TaskState.RETRY_WAIT,
        TaskState.RECONCILING, TaskState.NEEDS_REVIEW,
    }),
    TaskState.RETRY_WAIT: frozenset({TaskState.QUEUED}),
    TaskState.RECONCILING: frozenset({
        TaskState.SUCCEEDED, TaskState.FAILED,
        TaskState.RETRY_WAIT, TaskState.NEEDS_REVIEW,
    }),
    TaskState.NEEDS_REVIEW: frozenset(),
    TaskState.SUCCEEDED: frozenset(),
    TaskState.FAILED: frozenset(),
}


def validate_transition(current: TaskState, target: TaskState) -> None:
    if not isinstance(current, TaskState) or not isinstance(target, TaskState):
        raise ValueError("Transitions require TaskState values")
    if target not in _TRANSITIONS[current]:
        raise ValueError(f"Illegal transition: {current} -> {target}")


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    number: int
    worker_id: str
    attempt_id: UUID = field(default_factory=uuid4)
    started_at: datetime = field(default_factory=utc_now)
    finished_at: datetime | None = None
    outcome: AttemptOutcome = AttemptOutcome.RUNNING
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class TaskRecord:
    definition: TaskDefinition
    task_id: UUID = field(default_factory=uuid4)
    operation_id: UUID = field(default_factory=uuid4)
    state: TaskState = TaskState.QUEUED
    attempts: tuple[AttemptRecord, ...] = ()
    result_json: str | None = None

    @property
    def result(self) -> dict | None:
        return None if self.result_json is None else json.loads(self.result_json)
