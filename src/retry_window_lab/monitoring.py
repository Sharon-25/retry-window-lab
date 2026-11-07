"""Read-only snapshots from the durable queue schema."""

from contextlib import contextmanager

from psycopg import sql

from retry_window_lab.capacity import queue_limit
from retry_window_lab.db import DEFAULT_SCHEMA, connect, table, validate_schema
from retry_window_lab.domain import TaskState


class MonitoringRepository:
    def __init__(self, conninfo, schema=DEFAULT_SCHEMA):
        self.conninfo = conninfo
        self.schema = validate_schema(schema)

    def _table(self, name):
        return table(self.schema, name)

    @contextmanager
    def _snapshot(self):
        with connect(self.conninfo) as connection, connection.transaction():
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            yield connection

    def summary(self):
        with self._snapshot() as connection:
            states = dict.fromkeys((state.value for state in TaskState), 0)
            states.update((row["state"], row["total"]) for row in connection.execute(
                sql.SQL("SELECT state, count(*) AS total FROM {} GROUP BY state")
                .format(self._table("tasks"))
            ))
            outstanding = sum(total for state, total in states.items()
                              if state not in ("SUCCEEDED", "FAILED"))
            limit = queue_limit(connection, self.schema)
            ready = connection.execute(sql.SQL("""
                SELECT count(*) FILTER (WHERE ready) AS ready,
                       count(*) FILTER (WHERE NOT ready) AS awaiting_context,
                       count(*) FILTER (WHERE ready AND unpublished) AS pending_publication
                FROM (
                    SELECT (t.kind = 'checksum' OR pc.task_id IS NOT NULL) AS ready,
                           (o.event_id IS NOT NULL AND o.published_at IS NULL) AS unpublished
                    FROM {} AS t
                    LEFT JOIN {} AS pc ON pc.task_id = t.task_id
                    LEFT JOIN {} AS o ON o.task_id = t.task_id
                        AND o.delivery_generation = t.delivery_generation
                        AND o.event_type = 'TASK_READY'
                    WHERE t.state = 'QUEUED'
                ) AS queue
            """).format(self._table("tasks"), self._table("provider_context"),
                        self._table("outbox"))).fetchone()
            attempts = connection.execute(sql.SQL("""
                SELECT count(*) FILTER (WHERE error_code = 'LEASE_EXPIRED') AS expired_leases,
                       count(*) FILTER (WHERE purpose = 'RECONCILIATION') AS reconciliation_attempts
                FROM {}
            """).format(self._table("attempts"))).fetchone()
            retries = connection.execute(sql.SQL("""
                SELECT count(*) AS total FROM {}
                WHERE details->>'retry_delay_seconds' IS NOT NULL
            """).format(self._table("recovery_decisions"))).fetchone()["total"]
            worker_activity = connection.execute(sql.SQL("""
                WITH slots AS (
                    SELECT a.worker_id, a.purpose,
                           greatest(a.started_at, a.finished_at) AS activity_at,
                           coalesce(t.active_attempt_id = a.attempt_id
                               AND t.lease_until > transaction_timestamp(), false) AS active
                    FROM {} AS a JOIN {} AS t ON a.task_id = t.task_id
                )
                SELECT worker_id, purpose, max(activity_at) AS last_activity_at,
                       count(*) FILTER (WHERE active) AS active_leases,
                       count(*) FILTER (
                           WHERE activity_at >= transaction_timestamp() - interval '15 minutes'
                       ) AS recent_attempts
                FROM slots
                WHERE active OR activity_at >= transaction_timestamp() - interval '15 minutes'
                GROUP BY worker_id, purpose
                ORDER BY active_leases DESC, last_activity_at DESC, worker_id, purpose
                LIMIT 20
            """).format(self._table("attempts"), self._table("tasks"))).fetchall()
            active_leases = connection.execute(sql.SQL("""
                SELECT count(*) AS total FROM {}
                WHERE active_attempt_id IS NOT NULL AND lease_until > transaction_timestamp()
            """).format(self._table("tasks"))).fetchone()["total"]
            recent_tasks = connection.execute(sql.SQL("""
                SELECT task_id, kind, state, delivery_generation, attempt_count, updated_at
                FROM {} ORDER BY updated_at DESC, task_id LIMIT 24
            """).format(self._table("tasks"))).fetchall()
            captured_at = connection.execute(
                "SELECT transaction_timestamp() AS captured_at"
            ).fetchone()["captured_at"]
            return {
                "captured_at": captured_at,
                "capacity": {"outstanding": outstanding, "limit": limit,
                             "at_capacity": outstanding >= limit},
                "states": states,
                "queue": ready,
                "recovery": {**attempts, "retry_decisions": retries},
                "active_leases": active_leases,
                "workers": worker_activity,
                "recent_tasks": recent_tasks,
            }

    def task_history(self, task_id):
        with self._snapshot() as connection:
            task = connection.execute(sql.SQL("""
                SELECT task_id, operation_id, kind, state, delivery_generation, attempt_count,
                       mutation_attempts, observation_count, retry_due_at, recovery_due_at,
                       active_attempt_id, lease_until, created_at, updated_at
                FROM {} WHERE task_id = %s
            """).format(self._table("tasks")), (task_id,)).fetchone()
            if task is None:
                raise LookupError("Unknown task")
            attempts = connection.execute(sql.SQL("""
                SELECT attempt_id, number, lease_token, worker_id, purpose, started_at,
                       finished_at, outcome, error_code
                FROM {} WHERE task_id = %s ORDER BY number
            """).format(self._table("attempts")), (task_id,)).fetchall()
            dispatches = connection.execute(sql.SQL("""
                SELECT dispatch_id, attempt_id, started_at, settled_no_effect
                FROM {} WHERE task_id = %s ORDER BY started_at, dispatch_id
            """).format(self._table("provider_dispatches")), (task_id,)).fetchall()
            observations = connection.execute(sql.SQL("""
                SELECT observation_id, attempt_id, source, http_status, error_code, received_at
                FROM {} WHERE task_id = %s ORDER BY received_at, observation_id
            """).format(self._table("provider_observations")), (task_id,)).fetchall()
            decisions = connection.execute(sql.SQL("""
                SELECT decision_id, attempt_id, observation_id, reason, policy_version,
                       details, created_at
                FROM {} WHERE task_id = %s ORDER BY created_at, decision_id
            """).format(self._table("recovery_decisions")), (task_id,)).fetchall()
            return {
                "task": task, "attempts": attempts, "dispatches": dispatches,
                "observations": observations, "decisions": decisions,
            }
