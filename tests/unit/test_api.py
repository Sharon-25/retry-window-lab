from datetime import datetime, timezone
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient

from retry_window_lab.api import create_app
from retry_window_lab.dispatcher import OutboxDispatcher
from retry_window_lab.domain import TaskDefinition, TaskState
from retry_window_lab.local import SubmissionConflict
from retry_window_lab.storage import StoredTask, SubmissionResult


BODY = {"kind": "checksum", "submission_key": "api-test", "payload": {"text": "abc"}}


class StubRepository:
    def __init__(self, error=None, created=True):
        self.calls = 0
        self.error = error
        self.created = created
        self.task = StoredTask(
            task_id=uuid4(), operation_id=uuid4(),
            definition=TaskDefinition(**BODY), state=TaskState.QUEUED,
            delivery_generation=1, created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc), result_json=None,
        )

    def submit(self, definition):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return SubmissionResult(self.task, self.created)

    def get(self, task_id):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.task if task_id == self.task.task_id else None


@pytest.mark.parametrize("created,status", [(True, 202), (False, 200)])
def test_submission_status_and_location(created, status):
    repository = StubRepository(created=created)
    with TestClient(create_app(repository)) as client:
        response = client.post("/tasks", json=BODY)
        assert response.status_code == status
        assert response.headers["location"] == f"/tasks/{repository.task.task_id}"
        assert response.json()["created"] is created
        assert response.json()["task"]["state"] == "QUEUED"
        assert client.get(response.headers["location"]).status_code == 200


@pytest.mark.parametrize("body", [
    {},
    {**BODY, "kind": "unknown"},
    {**BODY, "submission_key": 123},
    {**BODY, "submission_key": " "},
    {**BODY, "extra": "unexpected"},
    {**BODY, "payload": []},
    {**BODY, "payload": {"text": 123}},
    {**BODY, "payload": {"text": "x" * 65_536}},
    {"kind": "reserve_inventory", "submission_key": "reservation", "payload": {
        "sku": "SKU-BOOK-001", "quantity": True, "provider_profile": "delayed",
    }},
])
def test_invalid_request_never_reaches_storage(body):
    repository = StubRepository()
    with TestClient(create_app(repository)) as client:
        assert client.post("/tasks", json=body).status_code == 422
    assert repository.calls == 0


def test_submission_conflict_response():
    with TestClient(create_app(StubRepository(error=SubmissionConflict()))) as client:
        response = client.post("/tasks", json=BODY)
    assert response.status_code == 409
    assert response.json() == {"detail": "submission_key_conflict"}


def test_database_error_is_sanitized():
    repository = StubRepository(error=psycopg.OperationalError("private connection details"))
    with TestClient(create_app(repository)) as client:
        response = client.post("/tasks", json=BODY)
        read = client.get(f"/tasks/{repository.task.task_id}")
    assert response.status_code == 503
    assert response.headers["retry-after"] == "1"
    assert response.json() == {"detail": "database_unavailable"}
    assert read.status_code == 503
    assert "private" not in response.text + read.text


def test_unknown_and_malformed_task_ids():
    repository = StubRepository()
    with TestClient(create_app(repository)) as client:
        assert client.get(f"/tasks/{uuid4()}").status_code == 404
        assert client.get("/tasks/not-a-uuid").status_code == 422
    assert repository.calls == 1


def test_health_is_liveness_without_database_access():
    repository = StubRepository(error=AssertionError("must not access storage"))
    with TestClient(create_app(repository)) as client:
        assert client.get("/health").json() == {"status": "ok"}
    assert repository.calls == 0


@pytest.mark.parametrize("batch_size", [0, -1, 1001, True, 1.5])
def test_invalid_batch_size_fails_before_connecting(batch_size):
    dispatcher = OutboxDispatcher("unused", None)
    with pytest.raises(ValueError):
        dispatcher.dispatch_once(batch_size)
