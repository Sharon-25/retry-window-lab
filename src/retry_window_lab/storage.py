import json
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4

from psycopg import sql
from psycopg.types.json import Jsonb

from retry_window_lab.db import DEFAULT_SCHEMA, connect, table, validate_schema
from retry_window_lab.domain import TaskDefinition, TaskState, canonical_json
from retry_window_lab.local import SubmissionConflict


@dataclass(frozen=True, slots=True)
class StoredTask:
    task_id: UUID
    operation_id: UUID
    definition: TaskDefinition
    state: TaskState
    delivery_generation: int
    created_at: datetime
    updated_at: datetime
    result_json: str | None

    def as_dict(self) -> dict:
        return {
            "task_id": str(self.task_id),
            "operation_id": str(self.operation_id),
            "submission_key": self.definition.submission_key,
            "kind": self.definition.kind.value,
            "payload": self.definition.payload,
            "fingerprint": self.definition.fingerprint,
            "state": self.state.value,
            "delivery_generation": self.delivery_generation,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "result": None if self.result_json is None else json.loads(self.result_json),
        }


@dataclass(frozen=True, slots=True)
class SubmissionResult:
    task: StoredTask
    created: bool


def _stored(row: dict) -> StoredTask:
    definition = TaskDefinition(row["kind"], row["submission_key"], row["payload"])
    if definition.fingerprint != row["fingerprint"]:
        raise RuntimeError("Stored task fingerprint does not match its definition")
    return StoredTask(
        task_id=row["task_id"], operation_id=row["operation_id"],
        definition=definition, state=TaskState(row["state"]),
        delivery_generation=row["delivery_generation"],
        created_at=row["created_at"], updated_at=row["updated_at"],
        result_json=None if row["result"] is None else canonical_json(row["result"]),
    )


class TaskRepository:
    def __init__(self, conninfo: str, schema: str = DEFAULT_SCHEMA):
        self._conninfo = conninfo
        self._schema = validate_schema(schema)

    def submit(self, definition: TaskDefinition) -> SubmissionResult:
        if not isinstance(definition, TaskDefinition):
            raise ValueError("submit requires a validated TaskDefinition")
        with connect(self._conninfo) as connection:
            with connection.transaction():
                row = connection.execute(
                    sql.SQL("""
                        INSERT INTO {} (
                            task_id, operation_id, submission_key,
                            kind, payload, fingerprint
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (submission_key) DO NOTHING
                        RETURNING *
                    """).format(table(self._schema, "tasks")),
                    (uuid4(), uuid4(), definition.submission_key,
                     definition.kind.value, Jsonb(definition.payload), definition.fingerprint),
                ).fetchone()

                if row is None:
                    # A separate statement obtains a fresh Read Committed snapshot.
                    row = connection.execute(
                        sql.SQL("SELECT * FROM {} WHERE submission_key = %s")
                        .format(table(self._schema, "tasks")),
                        (definition.submission_key,),
                    ).fetchone()
                    if row is None:
                        raise RuntimeError("Conflicting task disappeared; resubmit same key")
                    if row["fingerprint"] != definition.fingerprint:
                        raise SubmissionConflict("submission_key reused with different work")
                    result = SubmissionResult(task=_stored(row), created=False)
                else:
                    connection.execute(
                        sql.SQL("""
                            INSERT INTO {} (
                                event_id, task_id, delivery_generation, event_type
                            ) VALUES (%s, %s, %s, 'TASK_READY')
                        """).format(table(self._schema, "outbox")),
                        (uuid4(), row["task_id"], row["delivery_generation"]),
                    )
                    result = SubmissionResult(task=_stored(row), created=True)
            # Return acceptance only after the transaction context committed.
            return result

    def get(self, task_id: UUID) -> StoredTask | None:
        if not isinstance(task_id, UUID):
            raise ValueError("get requires a UUID")
        with connect(self._conninfo) as connection:
            row = connection.execute(
                sql.SQL("SELECT * FROM {} WHERE task_id = %s")
                .format(table(self._schema, "tasks")),
                (task_id,),
            ).fetchone()
            return None if row is None else _stored(row)
