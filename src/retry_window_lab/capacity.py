"""Persist and administer the outstanding task limit for one queue schema."""

import argparse
import json

from psycopg import sql

from retry_window_lab.config import postgres_conninfo
from retry_window_lab.db import DEFAULT_SCHEMA, connect, table

MAX_OUTSTANDING = 1_000_000


class QueueAtCapacity(Exception):
    """The queue rejected a new task before creating any durable records."""


def lock_admission(connection, schema):
    connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (f"retry-window-lab:admission:{schema}",),
    )


def queue_limit(connection, schema):
    return connection.execute(
        sql.SQL("SELECT outstanding_limit FROM {} WHERE singleton").format(
            table(schema, "queue_settings")
        )
    ).fetchone()["outstanding_limit"]


def set_limit(conninfo, limit, schema=DEFAULT_SCHEMA):
    if type(limit) is not int or not 1 <= limit <= MAX_OUTSTANDING:
        raise ValueError(f"Outstanding limit must be between 1 and {MAX_OUTSTANDING}")
    with connect(conninfo) as connection, connection.transaction():
        lock_admission(connection, schema)
        connection.execute(
            sql.SQL("UPDATE {} SET outstanding_limit=%s WHERE singleton").format(
                table(schema, "queue_settings")
            ),
            (limit,),
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    conninfo = postgres_conninfo()
    if args.limit is not None:
        try:
            set_limit(conninfo, args.limit, args.schema)
        except ValueError as error:
            parser.error(str(error))
    with connect(conninfo) as connection:
        print(json.dumps({"outstanding_limit": queue_limit(connection, args.schema)}))


if __name__ == "__main__":
    main()
