from uuid import uuid4

import psycopg
import pytest
from redis import Redis

from retry_window_lab.config import postgres_conninfo, redis_url


pytestmark = pytest.mark.integration


def test_postgres_transaction() -> None:
    with psycopg.connect(postgres_conninfo()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TEMP TABLE infrastructure_probe (
                    id INTEGER PRIMARY KEY,
                    value TEXT NOT NULL
                ) ON COMMIT DROP
                """
            )
            cursor.execute(
                "INSERT INTO infrastructure_probe VALUES (%s, %s)",
                (1, "postgres-ready"),
            )
            cursor.execute(
                "SELECT value FROM infrastructure_probe WHERE id = %s",
                (1,),
            )
            assert cursor.fetchone() == ("postgres-ready",)


def test_redis_stream_delivery_and_acknowledgement() -> None:
    stream = f"rwl:test:{uuid4().hex}"
    group = "infrastructure-probe"

    with Redis.from_url(
        redis_url(),
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    ) as client:
        try:
            assert client.ping() is True

            client.xgroup_create(stream, group, id="0", mkstream=True)
            message_id = client.xadd(stream, {"task_id": "probe-task"})

            deliveries = client.xreadgroup(
                group,
                "probe-worker",
                {stream: ">"},
                count=1,
                block=1000,
            )

            assert len(deliveries) == 1
            received_stream, messages = deliveries[0]
            assert received_stream == stream
            assert messages == [
                (message_id, {"task_id": "probe-task"})
            ]

            assert client.xpending(stream, group)["pending"] == 1
            assert client.xack(stream, group, message_id) == 1
            assert client.xpending(stream, group)["pending"] == 0
        finally:
            client.delete(stream)
