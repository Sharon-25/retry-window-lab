import argparse
import json
import math
import time
from dataclasses import asdict, dataclass

import psycopg
from psycopg import sql
from redis import Redis
from redis.exceptions import RedisError

from retry_window_lab.config import postgres_conninfo, redis_url
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
        if type(batch_size) is not int or not 1 <= batch_size <= 1000:
            raise ValueError("batch_size must be an integer from 1 to 1000")
        sent = marked = 0
        # Autocommit: no transaction or row lock is held during Redis I/O.
        with connect(self._conninfo) as connection:
            rows = connection.execute(
                sql.SQL("""
                    SELECT event_id, task_id, delivery_generation, event_type
                    FROM {} WHERE published_at IS NULL
                    ORDER BY created_at, event_id LIMIT %s
                """).format(table(self._schema, "outbox")),
                (batch_size,),
            ).fetchall()
            for row in rows:
                self._redis.xadd(self._stream, {
                    "protocol_version": "1",
                    "event_id": str(row["event_id"]),
                    "task_id": str(row["task_id"]),
                    "delivery_generation": str(row["delivery_generation"]),
                    "event_type": row["event_type"],
                })
                sent += 1
                # A crash after XADD can cause republishing of the same event_id.
                marked += connection.execute(
                    sql.SQL("""
                        UPDATE {} SET published_at = clock_timestamp()
                        WHERE event_id = %s AND published_at IS NULL
                    """).format(table(self._schema, "outbox")),
                    (row["event_id"],),
                ).rowcount
        return DispatchReport(len(rows), sent, marked)


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish pending outbox notifications")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 1000:
        parser.error("batch-size must be between 1 and 1000")
    if not math.isfinite(args.poll_seconds) or args.poll_seconds < 0.1:
        parser.error("poll-seconds must be finite and at least 0.1")

    with Redis.from_url(
        redis_url(), decode_responses=True,
        socket_connect_timeout=2, socket_timeout=2,
    ) as client:
        dispatcher = OutboxDispatcher(postgres_conninfo(), client)
        try:
            while True:
                try:
                    report = dispatcher.dispatch_once(args.batch_size)
                    print(json.dumps({"event": "dispatch", **asdict(report)}), flush=True)
                except (psycopg.Error, RedisError) as exc:
                    print(json.dumps({
                        "event": "dispatch_error", "error_type": type(exc).__name__,
                    }), flush=True)
                    if args.once:
                        raise SystemExit(1) from None
                if args.once:
                    break
                time.sleep(args.poll_seconds)
        except KeyboardInterrupt:
            print(json.dumps({"event": "dispatcher_stopped"}), flush=True)


if __name__ == "__main__":
    main()
