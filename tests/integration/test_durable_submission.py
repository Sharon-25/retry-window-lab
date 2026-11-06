from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from retry_window_lab.config import postgres_conninfo
from retry_window_lab.db import connect, table, validate_schema
from retry_window_lab.domain import TaskDefinition, TaskState
from retry_window_lab.local import SubmissionConflict
from retry_window_lab.migrate import MIGRATIONS, migrate
from retry_window_lab.storage import TaskRepository


pytestmark = pytest.mark.integration


@pytest.fixture
def database():
    conninfo = postgres_conninfo()
    schema = f"rwl_test_{uuid4().hex}"
    try:
        migrate(conninfo, schema)
        yield conninfo, schema
    finally:
        with connect(conninfo) as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
            )


def definition(text="abc", key="durable-key"):
    return TaskDefinition("checksum", key, {"text": text})


def counts(conninfo, schema):
    with connect(conninfo) as connection:
        return tuple(
            connection.execute(
                sql.SQL("SELECT count(*) AS n FROM {}").format(table(schema, name))
            ).fetchone()["n"]
            for name in ("tasks", "outbox")
        )


def test_migration_is_repeatable(database):
    conninfo, schema = database
    assert migrate(conninfo, schema) == []
    with connect(conninfo) as connection:
        versions = connection.execute(
            sql.SQL("SELECT version FROM {} ORDER BY version")
            .format(table(schema, "schema_migrations"))
        ).fetchall()
    assert [row["version"] for row in versions] == [version for version, _ in MIGRATIONS]


def test_modified_applied_migration_is_rejected(database):
    conninfo, schema = database
    with connect(conninfo) as connection:
        connection.execute(
            sql.SQL("UPDATE {} SET checksum = 'changed' WHERE version = 1")
            .format(table(schema, "schema_migrations"))
        )
    with pytest.raises(RuntimeError, match="modified"):
        migrate(conninfo, schema)


def test_unknown_migration_version_is_rejected(database):
    conninfo, schema = database
    with connect(conninfo) as connection:
        connection.execute(
            sql.SQL("INSERT INTO {} (version, checksum) VALUES (99, 'unknown')")
            .format(table(schema, "schema_migrations"))
        )
    with pytest.raises(RuntimeError, match="Unknown"):
        migrate(conninfo, schema)


def test_first_time_concurrent_migrations_serialize():
    conninfo = postgres_conninfo()
    schema = f"rwl_test_{uuid4().hex}"
    barrier = Barrier(4)

    def apply(_):
        barrier.wait(timeout=10)
        return migrate(conninfo, schema)

    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(apply, range(4)))
        assert results.count([version for version, _ in MIGRATIONS]) == 1
        assert results.count([]) == 3
    finally:
        with connect(conninfo) as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
            )


def test_failed_migration_rolls_back_schema_creation(monkeypatch):
    import retry_window_lab.migrate as migration_module

    conninfo = postgres_conninfo()
    schema = f"rwl_test_{uuid4().hex}"
    monkeypatch.setattr(migration_module, "MIGRATIONS", ((1, "SELECT 1 / 0;"),))
    with pytest.raises(psycopg.errors.DivisionByZero):
        migrate(conninfo, schema)
    with connect(conninfo) as connection:
        row = connection.execute(
            "SELECT count(*) AS n FROM pg_namespace WHERE nspname = %s", (schema,)
        ).fetchone()
    assert row["n"] == 0


def test_task_and_outbox_are_persisted_together(database):
    conninfo, schema = database
    result = TaskRepository(conninfo, schema).submit(definition())
    assert result.created is True
    assert result.task.state == TaskState.QUEUED
    assert result.task.delivery_generation == 1
    assert result.task.result_json is None
    assert counts(conninfo, schema) == (1, 1)
    with connect(conninfo) as connection:
        row = connection.execute(
            sql.SQL("SELECT * FROM {}").format(table(schema, "outbox"))
        ).fetchone()
    assert row["task_id"] == result.task.task_id
    assert row["delivery_generation"] == 1
    assert row["event_type"] == "TASK_READY"
    assert row["published_at"] is None


def test_new_repository_reads_committed_task(database):
    conninfo, schema = database
    result = TaskRepository(conninfo, schema).submit(definition())
    stored = TaskRepository(conninfo, schema).get(result.task.task_id)
    assert stored == result.task
    assert stored.as_dict()["payload"] == {"text": "abc"}
    assert stored.as_dict()["result"] is None
    assert stored.created_at.utcoffset().total_seconds() == 0


def test_matching_resubmission_creates_no_extra_event(database):
    conninfo, schema = database
    repository = TaskRepository(conninfo, schema)
    first = repository.submit(definition())
    second = repository.submit(definition())
    assert second.created is False
    assert second.task == first.task
    assert counts(conninfo, schema) == (1, 1)


def test_conflicting_submission_preserves_original(database):
    conninfo, schema = database
    repository = TaskRepository(conninfo, schema)
    first = repository.submit(definition())
    with pytest.raises(SubmissionConflict):
        repository.submit(definition("different"))
    assert repository.get(first.task.task_id) == first.task
    assert counts(conninfo, schema) == (1, 1)


def test_different_keys_represent_different_tasks(database):
    conninfo, schema = database
    repository = TaskRepository(conninfo, schema)
    first = repository.submit(definition(key="one"))
    second = repository.submit(definition(key="two"))
    assert first.task.task_id != second.task.task_id
    assert first.task.operation_id != second.task.operation_id
    assert counts(conninfo, schema) == (2, 2)


def test_outbox_insert_failure_rolls_back_task(database):
    conninfo, schema = database
    with connect(conninfo) as connection:
        connection.execute(sql.SQL("""
            CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION 'injected outbox failure';
            END;
            $$
        """).format(table(schema, "reject_outbox")))
        connection.execute(sql.SQL("""
            CREATE TRIGGER reject_outbox BEFORE INSERT ON {}
            FOR EACH ROW EXECUTE FUNCTION {}()
        """).format(table(schema, "outbox"), table(schema, "reject_outbox")))
    with pytest.raises(psycopg.errors.RaiseException, match="injected outbox failure"):
        TaskRepository(conninfo, schema).submit(definition())
    assert counts(conninfo, schema) == (0, 0)


def test_simultaneous_matching_submissions_converge(database):
    conninfo, schema = database
    barrier = Barrier(8)

    def submit(_):
        barrier.wait(timeout=10)
        return TaskRepository(conninfo, schema).submit(definition())

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(submit, range(8)))
    assert sum(result.created for result in results) == 1
    assert len({result.task.task_id for result in results}) == 1
    assert len({result.task.operation_id for result in results}) == 1
    assert counts(conninfo, schema) == (1, 1)


def test_simultaneous_conflicting_submissions_have_one_winner(database):
    conninfo, schema = database
    barrier = Barrier(2)

    def submit(text):
        barrier.wait(timeout=10)
        try:
            return TaskRepository(conninfo, schema).submit(definition(text))
        except SubmissionConflict:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(submit, ("first", "second")))
    assert sum(result == "conflict" for result in results) == 1
    assert sum(result != "conflict" and result.created for result in results) == 1
    assert counts(conninfo, schema) == (1, 1)


def test_unknown_task_returns_none(database):
    conninfo, schema = database
    assert TaskRepository(conninfo, schema).get(uuid4()) is None


def test_reservation_is_queued_without_external_execution(database):
    conninfo, schema = database
    request = TaskDefinition("reserve_inventory", "reservation-001", {
        "sku": "SKU-BOOK-001", "quantity": 2, "provider_profile": "delayed",
    })
    result = TaskRepository(conninfo, schema).submit(request)
    assert result.task.state == TaskState.QUEUED
    assert result.task.definition == request
    assert counts(conninfo, schema) == (1, 1)


def test_database_failure_is_not_reported_as_acceptance():
    # Unix socket path is deliberately nonexistent; no real host is contacted.
    repository = TaskRepository("host=/rwl-nonexistent-socket dbname=rwl connect_timeout=1")
    with pytest.raises(psycopg.OperationalError):
        repository.submit(definition())


@pytest.mark.parametrize("schema", [
    "public", "information_schema", "pg_catalog", "x;DROP", "UPPER", "", "x" * 64,
])
def test_unsafe_schema_names_are_rejected(schema):
    with pytest.raises(ValueError):
        validate_schema(schema)
