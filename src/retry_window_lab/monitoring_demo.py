"""Serve a dashboard backed by a separate queue with real simulator evidence."""

import argparse
import os
from threading import Event
from time import monotonic
from uuid import uuid4

from psycopg import sql
from redis import Redis
import uvicorn

from retry_window_lab.api import create_app
from retry_window_lab.capacity import set_limit
from retry_window_lab.config import postgres_conninfo, redis_url
from retry_window_lab.db import connect
from retry_window_lab.dispatcher import OutboxDispatcher
from retry_window_lab.domain import TaskDefinition, TaskState
from retry_window_lab.events import configure_event_output
from retry_window_lab.migrate import migrate
from retry_window_lab.ownership import OwnershipRepository
from retry_window_lab.provider import ProviderRepository
from retry_window_lab.provider_client import provider_url
from retry_window_lab.provision import grant_application_tables
from retry_window_lab.recovery import RecoveryRepository, RecoveryService
from retry_window_lab.runtime_config import application_conninfo
from retry_window_lab.storage import TaskRepository
from retry_window_lab.worker import Worker, report_tick


def seed(admin, app, schema, stream, client, base_url):
    store = TaskRepository(app, schema)
    ownership = OwnershipRepository(app, schema)
    recovery = RecoveryRepository(app, schema)
    provider = ProviderRepository(admin)
    dispatcher = OutboxDispatcher(app, client, schema, stream)
    worker = Worker(ownership, client, "dashboard-worker/slot-1", stream=stream)
    scanner = RecoveryService(recovery, worker_id="dashboard-recovery")

    expected = {}
    checksum = store.submit(TaskDefinition("checksum", "demo-checksum", {"text": "demo"})).task
    expected[checksum.task_id] = TaskState.SUCCEEDED
    for key, profile, fault, state in (
        ("demo-retry", "authoritative", "REJECT_NO_EFFECT", TaskState.SUCCEEDED),
        ("demo-reconciled", "authoritative", "RESPONSE_LOST", TaskState.SUCCEEDED),
        ("demo-review", "unavailable", "RESPONSE_LOST", TaskState.NEEDS_REVIEW),
    ):
        experiment = provider.create_experiment(profile)
        provider.inject(experiment, fault)
        task = store.submit(TaskDefinition("reserve_inventory", key, {
            "sku": "SKU-BOOK-001", "quantity": 2, "provider_profile": profile,
        })).task
        recovery.bind(task.task_id, experiment, base_url)
        expected[task.task_id] = state

    deadline = monotonic() + 45
    wait = Event()
    while monotonic() < deadline:
        recovery.schedule_due()
        dispatcher.dispatch_once()
        report_tick(worker)
        scanner.run_once()
        states = {task_id: store.get(task_id).state for task_id in expected}
        if states == expected:
            break
        wait.wait(0.05)
    else:
        raise RuntimeError(f"Dashboard scenarios did not settle as expected: {states}")

    for index in range(3):
        store.submit(TaskDefinition("checksum", f"demo-ready-{index}", {"text": str(index)}))
    return store


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8002)
    parser.add_argument("--base-url")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    admin = postgres_conninfo()
    app = application_conninfo()
    base_url = provider_url(args.base_url or os.environ.get("RWL_PROVIDER_URL", "http://127.0.0.1:8001"))
    schema = f"rwl_demo_{uuid4().hex}"
    stream = f"rwl:dashboard:{uuid4().hex}"
    configure_event_output()
    with Redis.from_url(redis_url(), decode_responses=True, socket_connect_timeout=2,
                        socket_timeout=2) as client:
        try:
            migrate(admin, schema)
            set_limit(admin, 4, schema)
            with connect(admin) as connection, connection.transaction():
                grant_application_tables(connection, schema, os.environ["RWL_APP_USER"])
            store = seed(admin, app, schema, stream, client, base_url)
            print(f"Open http://127.0.0.1:{args.port}/dashboard", flush=True)
            uvicorn.run(create_app(store), host="127.0.0.1", port=args.port)
        finally:
            try:
                client.delete(stream)
            finally:
                with connect(admin) as connection:
                    connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(schema)
                    ))


if __name__ == "__main__":
    main()
