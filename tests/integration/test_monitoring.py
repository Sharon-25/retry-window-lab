import json
import logging
from uuid import uuid4

from fastapi.testclient import TestClient
import psycopg
import pytest
from psycopg import sql

from retry_window_lab.api import create_app
from retry_window_lab.capacity import set_limit
from retry_window_lab.config import postgres_conninfo
from retry_window_lab.db import connect, table
from retry_window_lab.domain import TaskDefinition
from retry_window_lab.events import LOGGER_NAME
from retry_window_lab.migrate import migrate
from retry_window_lab.monitoring import MonitoringRepository
from retry_window_lab.ownership import Notification, OwnershipRepository
from retry_window_lab.storage import TaskRepository

pytestmark = pytest.mark.integration


@pytest.fixture
def queue():
    conninfo = postgres_conninfo()
    schema = f"rwl_test_{uuid4().hex}"
    try:
        migrate(conninfo, schema)
        yield conninfo, schema
    finally:
        with connect(conninfo) as connection:
            connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(schema)
            ))


def test_dashboard_shell_and_empty_snapshot(queue):
    with TestClient(create_app(TaskRepository(*queue))) as client:
        root = client.get("/", follow_redirects=False)
        assert root.status_code == 307 and root.headers["location"] == "/dashboard"
        dashboard = client.get(root.headers["location"])
        assert dashboard.status_code == 200 and "Queue monitor" in dashboard.text
        assert dashboard.headers["cache-control"] == "no-store"
        snapshot = client.get("/monitoring/summary").json()
        assert snapshot["capacity"] == {"outstanding": 0, "limit": 1000, "at_capacity": False}
        assert snapshot["queue"] == {"ready": 0, "awaiting_context": 0, "pending_publication": 0}
        assert all(count == 0 for count in snapshot["states"].values())
        assert snapshot["workers"] == [] and snapshot["recent_tasks"] == []


def test_capacity_and_worker_activity_follow_durable_transitions(queue, caplog):
    conninfo, schema = queue
    set_limit(conninfo, 2, schema)
    ownership = OwnershipRepository(conninfo, schema)
    store = TaskRepository(conninfo, schema)
    with TestClient(create_app(store)) as client, caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        checksum = {"kind": "checksum", "submission_key": "monitoring-secret-key",
                    "payload": {"text": "private-demo-payload"}}
        task_id = client.post("/tasks", json=checksum).json()["task"]["task_id"]
        reservation = {"kind": "reserve_inventory", "submission_key": "unbound",
                       "payload": {"sku": "SKU-BOOK-001", "quantity": 1,
                                   "provider_profile": "authoritative"}}
        assert client.post("/tasks", json=reservation).status_code == 202
        assert client.post("/tasks", json={**checksum, "submission_key": "third"}).status_code == 429
        snapshot = client.get("/monitoring/summary").json()
        assert snapshot["capacity"]["at_capacity"] is True
        assert snapshot["queue"] == {"ready": 1, "awaiting_context": 1, "pending_publication": 1}

        with connect(conninfo) as connection:
            event = connection.execute(sql.SQL("SELECT * FROM {} WHERE task_id=%s").format(
                table(schema, "outbox")
            ), (task_id,)).fetchone()
        notification = Notification(event["event_id"], event["task_id"], event["delivery_generation"])
        _, claim = ownership.claim(notification, "monitor-worker")
        snapshot = client.get("/monitoring/summary").json()
        assert snapshot["active_leases"] == 1 and snapshot["states"]["RUNNING"] == 1
        assert snapshot["workers"][0]["worker_id"] == "monitor-worker"
        assert snapshot["workers"][0]["active_leases"] == 1

        with connect(conninfo) as connection:
            connection.execute(sql.SQL("""
                UPDATE {} SET lease_until=clock_timestamp()-interval '1 second' WHERE task_id=%s
            """).format(table(schema, "tasks")), (task_id,))
        assert ownership.expire() == 1
        history_response = client.get(f"/monitoring/tasks/{task_id}")
        history = history_response.json()
        assert history["task"]["state"] == "NEEDS_REVIEW"
        assert history["attempts"][0]["attempt_id"] == str(claim.attempt_id)
        assert history["attempts"][0]["error_code"] == "LEASE_EXPIRED"
        assert "private-demo-payload" not in history_response.text
        assert "monitoring-secret-key" not in history_response.text
        snapshot = client.get("/monitoring/summary").json()
        assert snapshot["active_leases"] == 0
        assert snapshot["recovery"]["expired_leases"] == 1
        assert snapshot["capacity"]["at_capacity"] is True

    entries = [json.loads(record.getMessage()) for record in caplog.records if record.name == LOGGER_NAME]
    expired = next(entry for entry in entries if entry["event"] == "lease_expired")
    assert expired["task_id"] == task_id and expired["attempt_id"] == str(claim.attempt_id)
    assert expired["timestamp"].endswith("+00:00")
    assert any(entry["event"] == "submission_rejected" and entry["status"] == 429 for entry in entries)
    assert "private-demo-payload" not in caplog.text and "monitoring-secret-key" not in caplog.text


def test_monitoring_lookup_and_storage_unavailability(queue, monkeypatch):
    with TestClient(create_app(TaskRepository(*queue))) as client:
        assert client.get(f"/monitoring/tasks/{uuid4()}").status_code == 404
        assert client.get("/monitoring/tasks/invalid").status_code == 422

        def unavailable(_repo):
            raise psycopg.OperationalError("private database failure")

        monkeypatch.setattr(MonitoringRepository, "summary", unavailable)
        response = client.get("/monitoring/summary")
        assert response.status_code == 503
        assert response.json() == {"detail": "database_unavailable"}
