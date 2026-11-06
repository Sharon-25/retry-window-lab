from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from redis import Redis
from redis.exceptions import ConnectionError

from retry_window_lab.api import create_app
from retry_window_lab.config import postgres_conninfo, redis_url
from retry_window_lab.db import connect, table
from retry_window_lab.dispatcher import OutboxDispatcher
from retry_window_lab.domain import TaskDefinition
from retry_window_lab.migrate import migrate
from retry_window_lab.storage import TaskRepository


pytestmark = pytest.mark.integration


@pytest.fixture
def environment():
    conninfo = postgres_conninfo()
    schema = f"rwl_test_{uuid4().hex}"
    stream = f"rwl:test:{uuid4().hex}"
    with Redis.from_url(
        redis_url(), decode_responses=True, socket_connect_timeout=2, socket_timeout=2,
    ) as client:
        client.ping()
        try:
            migrate(conninfo, schema)
            yield conninfo, schema, stream, client
        finally:
            client.delete(stream)
            with connect(conninfo) as connection:
                connection.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE")
                    .format(sql.Identifier(schema))
                )


def submit(conninfo, schema, key="delivery-test"):
    return TaskRepository(conninfo, schema).submit(
        TaskDefinition("checksum", key, {"text": "abc"})
    )


def pending(conninfo, schema):
    with connect(conninfo) as connection:
        return connection.execute(
            sql.SQL("SELECT count(*) AS n FROM {} WHERE published_at IS NULL")
            .format(table(schema, "outbox"))
        ).fetchone()["n"]


def test_dispatch_publishes_notification_and_marks_outbox(environment):
    conninfo, schema, stream, client = environment
    task = submit(conninfo, schema).task
    report = OutboxDispatcher(conninfo, client, schema, stream).dispatch_once()
    assert (report.selected, report.notifications_sent, report.rows_marked) == (1, 1, 1)
    entries = client.xrange(stream)
    assert len(entries) == 1
    message = entries[0][1]
    assert message["protocol_version"] == "1"
    assert message["task_id"] == str(task.task_id)
    assert message["delivery_generation"] == "1"
    assert message["event_type"] == "TASK_READY"
    assert pending(conninfo, schema) == 0
    assert TaskRepository(conninfo, schema).get(task.task_id).state.value == "QUEUED"


def test_restart_does_not_republish_marked_events(environment):
    conninfo, schema, stream, client = environment
    submit(conninfo, schema)
    OutboxDispatcher(conninfo, client, schema, stream).dispatch_once()
    report = OutboxDispatcher(conninfo, client, schema, stream).dispatch_once()
    assert report.selected == 0
    assert client.xlen(stream) == 1


def test_redis_unavailable_preserves_pending_event(environment):
    conninfo, schema, stream, client = environment
    submit(conninfo, schema)
    with Redis.from_url("unix:///rwl-nonexistent-redis.sock") as unavailable:
        with pytest.raises(ConnectionError):
            OutboxDispatcher(conninfo, unavailable, schema, stream).dispatch_once()
    assert pending(conninfo, schema) == 1
    assert client.xlen(stream) == 0
    OutboxDispatcher(conninfo, client, schema, stream).dispatch_once()
    assert pending(conninfo, schema) == 0
    assert client.xlen(stream) == 1


def test_publish_then_mark_failure_can_duplicate_delivery(environment):
    conninfo, schema, stream, client = environment
    submit(conninfo, schema)
    with connect(conninfo) as connection:
        connection.execute(sql.SQL("""
            CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected mark failure'; END;
            $$
        """).format(table(schema, "reject_mark")))
        connection.execute(sql.SQL("""
            CREATE TRIGGER reject_mark BEFORE UPDATE ON {}
            FOR EACH ROW EXECUTE FUNCTION {}()
        """).format(table(schema, "outbox"), table(schema, "reject_mark")))
    with pytest.raises(psycopg.errors.RaiseException, match="injected mark failure"):
        OutboxDispatcher(conninfo, client, schema, stream).dispatch_once()
    assert client.xlen(stream) == 1
    assert pending(conninfo, schema) == 1
    with connect(conninfo) as connection:
        connection.execute(
            sql.SQL("DROP TRIGGER reject_mark ON {}").format(table(schema, "outbox"))
        )
    OutboxDispatcher(conninfo, client, schema, stream).dispatch_once()
    entries = client.xrange(stream)
    assert len(entries) == 2
    assert entries[0][0] != entries[1][0]
    assert entries[0][1] == entries[1][1]
    assert pending(conninfo, schema) == 0


def test_batch_limit_leaves_remaining_events_pending(environment):
    conninfo, schema, stream, client = environment
    for i in range(3):
        submit(conninfo, schema, f"batch-{i}")
    dispatcher = OutboxDispatcher(conninfo, client, schema, stream)
    assert dispatcher.dispatch_once(batch_size=2).selected == 2
    assert pending(conninfo, schema) == 1
    assert dispatcher.dispatch_once(batch_size=2).selected == 1
    assert pending(conninfo, schema) == 0
    assert client.xlen(stream) == 3


def test_partial_batch_failure_preserves_completed_progress(environment):
    conninfo, schema, stream, client = environment
    for i in range(3):
        submit(conninfo, schema, f"partial-{i}")

    class FailSecondPublish:
        calls = 0

        def xadd(self, name, fields):
            self.calls += 1
            if self.calls == 2:
                raise ConnectionError("injected second-call failure")
            return client.xadd(name, fields)

    with pytest.raises(ConnectionError):
        OutboxDispatcher(conninfo, FailSecondPublish(), schema, stream).dispatch_once()
    assert pending(conninfo, schema) == 2
    assert client.xlen(stream) == 1
    OutboxDispatcher(conninfo, client, schema, stream).dispatch_once()
    assert pending(conninfo, schema) == 0
    assert client.xlen(stream) == 3


def test_concurrent_dispatchers_can_publish_same_event(environment):
    conninfo, schema, stream, client = environment
    submit(conninfo, schema)
    barrier = Barrier(2)

    class SimultaneousPublish:
        def xadd(self, name, fields):
            barrier.wait(timeout=10)
            return client.xadd(name, fields)

    def dispatch(_):
        return OutboxDispatcher(conninfo, SimultaneousPublish(), schema, stream).dispatch_once()

    with ThreadPoolExecutor(max_workers=2) as pool:
        reports = list(pool.map(dispatch, range(2)))
    assert sum(report.rows_marked for report in reports) == 1
    assert client.xlen(stream) == 2
    entries = client.xrange(stream)
    assert entries[0][1] == entries[1][1]


def test_api_acceptance_readback_conflict_and_delivery(environment):
    conninfo, schema, stream, redis_client = environment
    repository = TaskRepository(conninfo, schema)
    body = {"kind": "checksum", "submission_key": "api-integration", "payload": {"text": "abc"}}
    with TestClient(create_app(repository)) as client:
        first = client.post("/tasks", json=body)
        second = client.post("/tasks", json=body)
        conflict = client.post("/tasks", json={**body, "payload": {"text": "changed"}})
        assert first.status_code == 202
        assert second.status_code == 200
        assert conflict.status_code == 409
        assert first.json()["task"] == second.json()["task"]
        stored = client.get(first.headers["location"])
        assert stored.status_code == 200
        assert stored.json() == first.json()["task"]
    assert pending(conninfo, schema) == 1
    OutboxDispatcher(conninfo, redis_client, schema, stream).dispatch_once()
    assert redis_client.xlen(stream) == 1


def test_api_still_accepts_when_redis_is_unavailable(environment):
    conninfo, schema, stream, client = environment
    with TestClient(create_app(TaskRepository(conninfo, schema))) as api:
        response = api.post("/tasks", json={
            "kind": "checksum", "submission_key": "redis-independent",
            "payload": {"text": "abc"},
        })
    assert response.status_code == 202
    with Redis.from_url("unix:///rwl-nonexistent-redis.sock") as unavailable:
        with pytest.raises(ConnectionError):
            OutboxDispatcher(conninfo, unavailable, schema, stream).dispatch_once()
    assert pending(conninfo, schema) == 1
    assert client.xlen(stream) == 0
