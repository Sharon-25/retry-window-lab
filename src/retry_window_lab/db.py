import re

import psycopg
from psycopg import IsolationLevel, sql
from psycopg.rows import dict_row


DEFAULT_SCHEMA = "rwl"


def validate_schema(name: str) -> str:
    if (not isinstance(name, str)
            or re.fullmatch(r"[a-z][a-z0-9_]{0,62}", name) is None
            or name in {"public", "information_schema"} or name.startswith("pg_")):
        raise ValueError("Use a dedicated lowercase application schema")
    return name


def table(schema: str, name: str) -> sql.Identifier:
    return sql.Identifier(validate_schema(schema), name)


def connect(conninfo: str) -> psycopg.Connection:
    connection = psycopg.connect(
        conninfo, autocommit=True, row_factory=dict_row,
        options="-c statement_timeout=10000 -c lock_timeout=5000 -c timezone=UTC",
    )
    connection.isolation_level = IsolationLevel.READ_COMMITTED
    return connection
