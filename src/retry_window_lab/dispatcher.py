import argparse
import math
import time
from dataclasses import asdict, dataclass

import psycopg
from psycopg import sql
from redis import Redis
from redis.exceptions import RedisError

from retry_window_lab.config import redis_url
from retry_window_lab.events import configure_event_output, emit_event
from retry_window_lab.runtime_config import application_conninfo
from retry_window_lab.db import DEFAULT_SCHEMA, connect, table, validate_schema


READY_STREAM = "rwl:ready"


@dataclass(frozen=True, slots=True)
class DispatchReport:
    selected: int
    notifications_sent: int
    rows_marked: int


class OutboxDispatcher:
    def __init__(
        self, conninfo: str, redis_client: Redis,
        schema: str = DEFAULT_SCHEMA, stream: str = READY_STREAM,
    ):
        self._conninfo = conninfo
        self._redis = redis_client
        self._schema = validate_schema(schema)
        if not isinstance(stream, str) or not stream.strip() or len(stream) > 128:
            raise ValueError("stream must contain 1 to 128 characters")
        self._stream = stream

    def dispatch_once(self, batch_size: int = 100) -> DispatchReport:
        self._validate_batch_size(batch_size)
        with connect(self._conninfo) as connection:
            rows = connection.execute(
                sql.SQL("""
                    SELECT event_id, task_id, delivery_generation, event_type
                    FROM {} WHERE published_at IS NULL
                    ORDER BY created_at, event_id LIMIT %s
                """).format(table(self._schema, "outbox")),
                (batch_size,),
            ).fetchall()
            return self._publish(connection, rows)

    @staticmethod
    def _validate_batch_size(batch_size):
        if type(batch_size) is not int or not 1 <= batch_size <= 1000:
            raise ValueError("batch_size must be an integer from 1 to 1000")

    def _publish(self, connection, rows):
        marked = 0
        for row in rows:
            self._redis.xadd(self._stream, {
                "protocol_version": "1",
                "event_id": str(row["event_id"]),
                "task_id": str(row["task_id"]),
                "delivery_generation": str(row["delivery_generation"]),
                "event_type": row["event_type"],
            })
            # A crash after XADD can cause republishing of the same event_id.
            marked += connection.execute(
                sql.SQL("""
                    UPDATE {} SET published_at = clock_timestamp()
                    WHERE event_id = %s AND published_at IS NULL
                """).format(table(self._schema, "outbox")),
                (row["event_id"],),
            ).rowcount
        return DispatchReport(len(rows), len(rows), marked)

    def replay_ready(self, batch_size=100):
        """Recreate eligible notifications from current durable outbox rows."""
        from retry_window_lab.recovery import RecoveryRepository

        self._validate_batch_size(batch_size)
        recovery = RecoveryRepository(self._conninfo, self._schema)
        with connect(self._conninfo) as connection:
            cutoff = connection.execute("SELECT clock_timestamp() AS cutoff").fetchone()["cutoff"]
        # Only retry deadlines already due at the start belong to this recovery pass.
        while recovery.schedule_due(batch_size, due_before=cutoff) == batch_size:
            pass

        selected = sent = marked = 0
        cursor = None
        # Each query and update autocommits. Network calls hold no database locks.
        with connect(self._conninfo) as connection:
            horizon = connection.execute("SELECT clock_timestamp() AS cutoff").fetchone()["cutoff"]
            while True:
                seek = sql.SQL("") if cursor is None else sql.SQL(
                    "AND (o.created_at, o.event_id) > (%s, %s)"
                )
                parameters = (horizon, *(cursor or ()), batch_size)
                rows = connection.execute(
                    sql.SQL("""
                        SELECT o.event_id, o.task_id, o.delivery_generation,
                               o.event_type, o.created_at
                        FROM {} AS o
                        JOIN {} AS t ON t.task_id = o.task_id
                            AND t.delivery_generation = o.delivery_generation
                        WHERE t.state = 'QUEUED' AND o.event_type = 'TASK_READY'
                          AND o.created_at <= %s
                          AND (t.kind = 'checksum' OR EXISTS (
                              SELECT 1 FROM {} AS pc WHERE pc.task_id = t.task_id
                          ))
                          {}
                        ORDER BY o.created_at, o.event_id LIMIT %s
                    """).format(table(self._schema, "outbox"),
                                table(self._schema, "tasks"),
                                table(self._schema, "provider_context"), seek),
                    parameters,
                ).fetchall()
                report = self._publish(connection, rows)
                selected += report.selected
                sent += report.notifications_sent
                marked += report.rows_marked
                if len(rows) < batch_size:
                    return DispatchReport(selected, sent, marked)
                cursor = rows[-1]["created_at"], rows[-1]["event_id"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish pending outbox notifications")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--replay-ready", action="store_true",
                      help="Rebuild current ready notifications after Redis loss")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 1000:
        parser.error("batch-size must be between 1 and 1000")
    if not math.isfinite(args.poll_seconds) or args.poll_seconds < 0.1:
        parser.error("poll-seconds must be finite and at least 0.1")

    configure_event_output()
    with Redis.from_url(
        redis_url(), decode_responses=True,
        socket_connect_timeout=2, socket_timeout=2,
    ) as client:
        dispatcher = OutboxDispatcher(application_conninfo(), client)
        try:
            if args.replay_ready:
                try:
                    report = dispatcher.replay_ready(args.batch_size)
                except (psycopg.Error, RedisError) as exc:
                    emit_event("replay_error", error_type=type(exc).__name__)
                    raise SystemExit(1) from None
                emit_event("replay_ready", **asdict(report))
                return
            while True:
                try:
                    report = dispatcher.dispatch_once(args.batch_size)
                    emit_event("dispatch", **asdict(report))
                except (psycopg.Error, RedisError) as exc:
                    emit_event("dispatch_error", error_type=type(exc).__name__)
                    if args.once:
                        raise SystemExit(1) from None
                if args.once:
                    break
                time.sleep(args.poll_seconds)
        except KeyboardInterrupt:
            emit_event("dispatcher_stopped")


if __name__ == "__main__":
    main()
