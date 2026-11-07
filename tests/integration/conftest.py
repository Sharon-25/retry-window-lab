"""Fixtures for upgrade tests that insert rows using the legacy schema contract."""

from uuid import uuid4

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

from retry_window_lab.db import connect, table
from retry_window_lab.domain import TaskDefinition
from retry_window_lab.storage import _stored


@pytest.fixture
def legacy_checksum_task():
    def insert(conninfo, schema):
        definition = TaskDefinition("checksum", "upgrade", {"text": "abc"})
        with connect(conninfo) as connection, connection.transaction():
            row = connection.execute(
                sql.SQL("""
                    INSERT INTO {} (task_id, operation_id, submission_key, kind, payload, fingerprint)
                    VALUES (%s,%s,%s,%s,%s,%s) RETURNING *
                """).format(table(schema, "tasks")),
                (uuid4(), uuid4(), definition.submission_key, definition.kind.value,
                 Jsonb(definition.payload), definition.fingerprint),
            ).fetchone()
            connection.execute(
                sql.SQL("""
                    INSERT INTO {} (event_id, task_id, delivery_generation, event_type)
                    VALUES (%s,%s,1,'TASK_READY')
                """).format(table(schema, "outbox")),
                (uuid4(), row["task_id"]),
            )
            return _stored(row)
    return insert
