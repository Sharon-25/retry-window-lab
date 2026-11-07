from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest
from psycopg import sql
from redis import Redis
from redis.exceptions import ConnectionError

from retry_window_lab.api import create_app
from retry_window_lab.capacity import QueueAtCapacity, queue_limit, set_limit
from retry_window_lab.capacity_demo import run_demo
from retry_window_lab.config import postgres_conninfo, redis_url
from retry_window_lab.db import connect, table
from retry_window_lab.dispatcher import OutboxDispatcher
from retry_window_lab.domain import TaskDefinition
from retry_window_lab.migrate import migrate
from retry_window_lab.ownership import OwnershipRepository
from retry_window_lab.recovery import RecoveryRepository
from retry_window_lab.storage import TaskRepository
from retry_window_lab.worker import Worker

pytestmark = pytest.mark.integration


@pytest.fixture
def queue():
    conninfo = postgres_conninfo()
    schema = f"rwl_test_{uuid4().hex}"
    stream = f"rwl:test:{uuid4().hex}"
    with Redis.from_url(redis_url(), decode_responses=True, socket_connect_timeout=2,
                        socket_timeout=2) as redis:
        try:
            migrate(conninfo, schema)
            yield conninfo, schema, stream, redis
        finally:
            try:
                redis.delete(stream)
            finally:
                with connect(conninfo) as connection:
                    connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(schema)
                    ))


def checksum(key, text="abc"):
    return TaskDefinition("checksum", key, {"text": text})


def counts(conninfo, schema):
    with connect(conninfo) as connection:
        return tuple(connection.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(
            table(schema, name)
        )).fetchone()["n"] for name in ("tasks", "outbox", "attempts"))


def test_concurrent_submissions_share_one_capacity_limit(queue):
    conninfo, schema, *_ = queue
    set_limit(conninfo, 3, schema)
    start = Barrier(8)

    def submit(number):
        start.wait(timeout=15)
        try:
            return TaskRepository(conninfo, schema).submit(checksum(f"new-{number}")).created
        except QueueAtCapacity:
            return False

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(submit, range(8)))

    assert results.count(True) == 3
    assert counts(conninfo, schema) == (3, 3, 0)


def test_http_backpressure_preserves_idempotency_and_releases_capacity(queue):
    conninfo, schema, stream, redis = queue
    set_limit(conninfo, 1, schema)
    body = {"kind": "checksum", "submission_key": "first", "payload": {"text": "abc"}}
    with TestClient(create_app(TaskRepository(conninfo, schema))) as api:
        accepted = api.post("/tasks", json=body)
        assert accepted.status_code == 202
        full = api.post("/tasks", json={**body, "submission_key": "second"})
        assert full.status_code == 429 and full.headers["Retry-After"] == "1"
        assert full.json()["detail"] == "queue_at_capacity"
        existing = api.post("/tasks", json=body)
        assert existing.status_code == 200
        assert existing.json()["task"]["task_id"] == accepted.json()["task"]["task_id"]
        assert api.post("/tasks", json={**body, "payload": {"text": "changed"}}).status_code == 409
        assert counts(conninfo, schema) == (1, 1, 0)

        OutboxDispatcher(conninfo, redis, schema, stream).dispatch_once()
        worker = Worker(OwnershipRepository(conninfo, schema), redis, "capacity-api", stream=stream)
        assert worker.tick()["status"] == "SUCCEEDED"
        assert api.post("/tasks", json={**body, "submission_key": "second"}).status_code == 202


def test_all_outstanding_states_count_and_terminal_states_release_slots(queue):
    conninfo, schema, *_ = queue
    store = TaskRepository(conninfo, schema)
    states = ("QUEUED", "RUNNING", "RETRY_WAIT", "RECONCILING", "NEEDS_REVIEW",
              "SUCCEEDED", "FAILED")
    with connect(conninfo) as connection:
        for state in states:
            task = store.submit(checksum(state)).task
            connection.execute(sql.SQL("UPDATE {} SET state=%s WHERE task_id=%s").format(
                table(schema, "tasks")
            ), (state, task.task_id))
    set_limit(conninfo, 5, schema)
    with pytest.raises(QueueAtCapacity):
        store.submit(checksum("new"))
    set_limit(conninfo, 6, schema)
    assert store.submit(checksum("new")).created
    assert counts(conninfo, schema) == (8, 8, 0)


def test_migration_4_preserves_preexisting_task_and_outbox(queue, monkeypatch, legacy_checksum_task):
    import retry_window_lab.migrate as module

    conninfo, *_ = queue
    schema = f"rwl_test_{uuid4().hex}"
    versions = module.MIGRATIONS
    try:
        monkeypatch.setattr(module, "MIGRATIONS", versions[:3])
        assert module.migrate(conninfo, schema) == [1, 2, 3]
        task = legacy_checksum_task(conninfo, schema)
        monkeypatch.setattr(module, "MIGRATIONS", versions)
        assert module.migrate(conninfo, schema) == [4]
        assert TaskRepository(conninfo, schema).get(task.task_id) == task
        assert counts(conninfo, schema) == (1, 1, 0)
        with connect(conninfo) as connection:
            assert queue_limit(connection, schema) == 1000
    finally:
        with connect(conninfo) as connection:
            connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(schema)
            ))


def test_replay_selects_current_ready_generations_and_materializes_due_retries(queue):
    conninfo, schema, stream, redis = queue
    store = TaskRepository(conninfo, schema)
    recovery = RecoveryRepository(conninfo, schema)
    dispatcher = OutboxDispatcher(conninfo, redis, schema, stream)
    checksum_task = store.submit(checksum("ready")).task

    def reserve(key, bind=True):
        task = store.submit(TaskDefinition("reserve_inventory", key, {
            "sku": "SKU-BOOK-001", "quantity": 2, "provider_profile": "authoritative",
        })).task
        if bind:
            recovery.bind(task.task_id, uuid4())
        return task

    ready = reserve("current")
    due = reserve("due")
    later = reserve("later")
    reserve("unbound", bind=False)
    completed = store.submit(checksum("completed")).task
    with connect(conninfo) as connection:
        statement = sql.SQL("UPDATE {} SET state=%s, retry_due_at=%s WHERE task_id=%s").format(
            table(schema, "tasks")
        )
        connection.execute(statement, ("SUCCEEDED", None, completed.task_id))
        connection.execute(statement, ("RETRY_WAIT", "2000-01-01T00:00:00Z", due.task_id))
        connection.execute(statement, ("RETRY_WAIT", "2999-01-01T00:00:00Z", later.task_id))

    dispatcher.dispatch_once()
    redis.delete(stream)
    report = dispatcher.replay_ready(batch_size=1)
    published = [(item["task_id"], item["delivery_generation"])
                 for _, item in redis.xrange(stream)]
    assert set(published) == {(str(checksum_task.task_id), "1"), (str(ready.task_id), "2"),
                              (str(due.task_id), "3")}
    assert len(published) == report.notifications_sent == 3
    assert report.rows_marked == 1
    assert store.get(later.task_id).state.value == "RETRY_WAIT"


def test_interrupted_replay_can_be_repeated_without_duplicate_execution(queue):
    conninfo, schema, stream, redis = queue
    store = TaskRepository(conninfo, schema)
    tasks = [store.submit(checksum(str(index))).task for index in range(3)]
    dispatcher = OutboxDispatcher(conninfo, redis, schema, stream)
    dispatcher.dispatch_once()
    redis.delete(stream)

    class InterruptAfterOne:
        writes = 0

        def xadd(self, name, fields):
            if self.writes:
                raise ConnectionError("test disconnect")
            self.writes += 1
            return redis.xadd(name, fields)

    with pytest.raises(ConnectionError):
        OutboxDispatcher(conninfo, InterruptAfterOne(), schema, stream).replay_ready()
    assert redis.xlen(stream) == 1
    assert dispatcher.replay_ready().notifications_sent == 3
    worker = Worker(OwnershipRepository(conninfo, schema), redis, "replay", stream=stream)
    for _ in range(4):
        worker.tick()
    assert all(store.get(task.task_id).state.value == "SUCCEEDED" for task in tasks)
    assert counts(conninfo, schema) == (3, 3, 3)


def test_worker_recreates_deleted_group_and_revalidates_old_notifications(queue):
    conninfo, schema, stream, redis = queue
    store = TaskRepository(conninfo, schema)
    dispatcher = OutboxDispatcher(conninfo, redis, schema, stream)
    worker = Worker(OwnershipRepository(conninfo, schema), redis, "group-repair", stream=stream)
    store.submit(checksum("first"))
    dispatcher.dispatch_once()
    assert worker.tick()["status"] == "SUCCEEDED"
    redis.xgroup_destroy(stream, worker.group)
    store.submit(checksum("second"))
    dispatcher.dispatch_once()
    worker.tick()
    assert worker.tick()["status"] == "SUCCEEDED"
    assert counts(conninfo, schema) == (2, 2, 2)


def test_live_worker_processes_keep_recovery_capacity_under_load():
    conninfo = postgres_conninfo()
    result = run_demo(conninfo, conninfo, redis_url())
    assert result["peak_execution_slots_observed"] == 4
    assert result["recovery_progressed_while_execution_slots_busy"]
    assert result["ready_notifications_rebuilt"] == 6
    assert result["completed"] == 8
    assert result["interrupted_attempts_in_review"] == 2
