"""Exercise two real worker processes with a controlled checksum workload."""

from collections import Counter
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import time
from uuid import uuid4

from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from redis import Redis

from retry_window_lab.capacity import QueueAtCapacity, set_limit
from retry_window_lab.config import postgres_conninfo, redis_url
from retry_window_lab.db import connect, table
from retry_window_lab.dispatcher import OutboxDispatcher
from retry_window_lab.domain import TaskDefinition
from retry_window_lab.local import checksum_handler
from retry_window_lab.migrate import migrate
from retry_window_lab.provision import grant_application_tables
from retry_window_lab.runtime_config import application_conninfo
from retry_window_lab.storage import TaskRepository
from retry_window_lab.worker import GROUP


def controlled_worker():
    """Pause designated demo handlers while running the normal worker CLI."""
    from retry_window_lab import worker

    release = Path(os.environ["RWL_DEMO_RELEASE_FILE"])
    execute = worker.execute_claim

    def checksum(payload):
        if payload["text"] == "hold":
            deadline = time.monotonic() + 60
            while not release.exists():
                if time.monotonic() >= deadline:
                    raise RuntimeError("Demo gate was not released")
                time.sleep(0.02)
        return checksum_handler(payload)

    def execute_controlled(repository, claim, lease_seconds):
        return execute(repository, claim, lease_seconds, handler=checksum)

    worker.execute_claim = execute_controlled
    worker.main()


def run_demo(admin, app, redis_location):
    schema = f"rwl_demo_{uuid4().hex}"
    stream = f"rwl:demo:{uuid4().hex}"
    credentials = conninfo_to_dict(app)
    role = credentials["user"]
    with TemporaryDirectory(prefix="rwl-capacity-") as directory, Redis.from_url(
        redis_location, decode_responses=True, socket_connect_timeout=2, socket_timeout=2,
    ) as redis:
        release = Path(directory) / "release"
        children = []

        def start(name):
            environment = dict(os.environ)
            environment.update({
                "RWL_APP_USER": role,
                "RWL_APP_PASSWORD": credentials.get("password", ""),
                "RWL_DEMO_RELEASE_FILE": str(release),
                "REDIS_URL": redis_location,
            })
            log = Path(directory) / f"{name}.log"
            with log.open("w") as output:
                process = subprocess.Popen(
                    [sys.executable, "-c",
                     "from retry_window_lab.capacity_demo import controlled_worker; controlled_worker()",
                     "--worker-id", name, "--concurrency", "2", "--lease-seconds", "3",
                     "--schema", schema, "--stream", stream],
                    env=environment, stdout=output, stderr=subprocess.STDOUT,
                )
            children.append((process, log))
            return process

        def states():
            with connect(app) as connection:
                rows = connection.execute(sql.SQL("SELECT state FROM {}").format(
                    table(schema, "tasks")
                )).fetchall()
                return Counter(row["state"] for row in rows)

        def wait_for(predicate, active):
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                for process, log in children:
                    if process in active and process.poll() is not None:
                        raise RuntimeError(f"Demo worker stopped early\n{log.read_text()[-4000:]}")
                current = states()
                if predicate(current):
                    return current
                time.sleep(0.05)
            raise RuntimeError(f"Demo timed out with states {states()}")

        try:
            migrate(admin, schema)
            set_limit(admin, 10, schema)
            with connect(admin) as connection, connection.transaction():
                grant_application_tables(connection, schema, role)
            store = TaskRepository(app, schema)
            dispatcher = OutboxDispatcher(app, redis, schema, stream)

            def definition(number):
                return TaskDefinition("checksum", f"demo-{number}",
                                      {"text": "hold" if number < 4 else f"quick-{number}"})

            for number in range(10):
                store.submit(definition(number))
            try:
                store.submit(definition(10))
            except QueueAtCapacity:
                pass
            else:
                raise AssertionError("Capacity should reject the eleventh task")
            assert not store.submit(definition(0)).created

            dispatcher.dispatch_once()
            first = start("demo-a")
            wait_for(lambda counts: counts == {"RUNNING": 2, "QUEUED": 8}, {first})
            second = start("demo-b")
            wait_for(lambda counts: counts == {"RUNNING": 4, "QUEUED": 6}
                     and redis.xpending(stream, GROUP)["pending"] == 0, {first, second})

            # All four execution slots are occupied. Simulate loss of this isolated stream.
            redis.delete(stream)
            rebuilt = dispatcher.replay_ready(batch_size=2)
            duplicate = dispatcher.replay_ready(batch_size=2)
            assert rebuilt.notifications_sent == duplicate.notifications_sent == 6
            first.kill()
            first.wait(timeout=5)
            wait_for(lambda counts: counts == {
                "RUNNING": 2, "QUEUED": 6, "NEEDS_REVIEW": 2,
            }, {second})
            release.touch()

            wait_for(lambda counts: counts == {"SUCCEEDED": 8, "NEEDS_REVIEW": 2}, {second})
            with connect(app) as connection:
                attempts = connection.execute(sql.SQL("""
                    SELECT worker_id, outcome, error_code FROM {}
                """).format(table(schema, "attempts"))).fetchall()
            distribution = Counter(row["worker_id"].split("/")[0] for row in attempts)
            assert distribution == {"demo-a": 2, "demo-b": 8}
            expired = [row for row in attempts if row["error_code"] == "LEASE_EXPIRED"]
            assert len(expired) == 2 and all(row["outcome"] == "UNKNOWN" for row in expired)
            assert len(attempts) == 10
            return {
                "capacity_rejection_verified": True,
                "idempotent_replay_at_capacity": True,
                "peak_execution_slots_observed": 4,
                "recovery_progressed_while_execution_slots_busy": True,
                "ready_notifications_rebuilt": rebuilt.notifications_sent,
                "duplicate_notifications_injected": duplicate.notifications_sent,
                "completed": 8,
                "interrupted_attempts_in_review": 2,
                "attempts_by_process": dict(sorted(distribution.items())),
            }
        finally:
            release.touch()
            for process, _ in children:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
            try:
                redis.delete(stream)
            finally:
                with connect(admin) as connection:
                    connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(schema)
                    ))


def main():
    print(json.dumps(run_demo(postgres_conninfo(), application_conninfo(), redis_url()), indent=2))


if __name__ == "__main__":
    main()
