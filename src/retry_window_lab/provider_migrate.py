import argparse
import hashlib

from psycopg import sql

from retry_window_lab.config import postgres_conninfo
from retry_window_lab.db import DEFAULT_SCHEMA, connect, table, validate_schema


PROVIDER_SCHEMA = "rwl_provider"
PROVIDER_SQL = """
CREATE TABLE experiments (
    experiment_id UUID PRIMARY KEY,
    profile TEXT NOT NULL CHECK(profile IN ('authoritative','delayed','unavailable')),
    profile_version INTEGER NOT NULL DEFAULT 1 CHECK(profile_version=1),
    window_seconds BIGINT NOT NULL CHECK(window_seconds BETWEEN 1 AND 3600),
    visibility_delay BIGINT NOT NULL CHECK(visibility_delay BETWEEN 0 AND 3600),
    now_seconds BIGINT NOT NULL DEFAULT 0 CHECK(now_seconds BETWEEN 0 AND 1000000000000),
    next_fault TEXT NOT NULL DEFAULT 'NONE' CHECK(next_fault IN ('NONE','RESPONSE_LOST','REJECT_NO_EFFECT','DELAY_DELIVERY')),
    next_delay BIGINT NOT NULL DEFAULT 0 CHECK(next_delay BETWEEN 0 AND 3600)
);
CREATE TABLE inventory (
    experiment_id UUID NOT NULL REFERENCES experiments(experiment_id) ON DELETE CASCADE,
    sku TEXT NOT NULL CHECK(sku IN ('SKU-BOOK-001','SKU-PEN-001')),
    remaining INTEGER NOT NULL CHECK(remaining BETWEEN 0 AND 100),
    PRIMARY KEY(experiment_id,sku)
);
CREATE TABLE reservations (
    reservation_id UUID PRIMARY KEY,
    experiment_id UUID NOT NULL REFERENCES experiments(experiment_id) ON DELETE CASCADE,
    operation_id UUID NOT NULL,
    idempotency_key TEXT NOT NULL,
    fingerprint TEXT NOT NULL CHECK(fingerprint ~ '^[0-9a-f]{64}$'),
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK(quantity>0),
    accepted_at BIGINT NOT NULL CHECK(accepted_at>=0),
    visible_at BIGINT NOT NULL CHECK(visible_at>=accepted_at)
);
CREATE INDEX reservations_operation_idx ON reservations(experiment_id,operation_id);
CREATE TABLE protection_keys (
    experiment_id UUID NOT NULL REFERENCES experiments(experiment_id) ON DELETE CASCADE,
    operation_id UUID NOT NULL,
    idempotency_key TEXT NOT NULL CHECK(char_length(idempotency_key) BETWEEN 1 AND 128),
    fingerprint TEXT NOT NULL CHECK(fingerprint ~ '^[0-9a-f]{64}$'),
    reservation_id UUID NOT NULL REFERENCES reservations(reservation_id),
    accepted_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL CHECK(expires_at>accepted_at),
    PRIMARY KEY(experiment_id,operation_id,idempotency_key)
);
CREATE TABLE requests (
    request_id UUID PRIMARY KEY,
    experiment_id UUID NOT NULL REFERENCES experiments(experiment_id) ON DELETE CASCADE,
    operation_id UUID NOT NULL,
    idempotency_key TEXT NOT NULL,
    fingerprint TEXT NOT NULL CHECK(fingerprint ~ '^[0-9a-f]{64}$'),
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK(quantity>0),
    due_at BIGINT NOT NULL CHECK(due_at>=0),
    status TEXT NOT NULL DEFAULT 'PROCESSING' CHECK(status IN ('PROCESSING','SUCCEEDED','REJECTED','CONFLICT')),
    reservation_id UUID REFERENCES reservations(reservation_id),
    accepted_at BIGINT,
    expires_at BIGINT,
    error_code TEXT CHECK(error_code IS NULL OR error_code IN ('INSUFFICIENT_STOCK','SETTLED_NO_EFFECT','KEY_CONFLICT')),
    reused BOOLEAN NOT NULL DEFAULT FALSE,
    CHECK((status='SUCCEEDED') = (reservation_id IS NOT NULL))
);
CREATE INDEX requests_due_idx ON requests(experiment_id,due_at,request_id) WHERE status='PROCESSING';
"""
MIGRATIONS = ((1, PROVIDER_SQL),)

def migrate(conninfo: str, schema: str = PROVIDER_SCHEMA) -> list[int]:
    schema = validate_schema(schema)
    applied = []
    with connect(conninfo) as connection:
        with connection.transaction():
            # Acquire before schema creation so competing migrators serialize.
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"retry-window-lab:provider-migrate:{schema}",),
            )
            connection.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
                .format(sql.Identifier(schema))
            )
            connection.execute(
                sql.SQL("SET LOCAL search_path TO {}, pg_catalog")
                .format(sql.Identifier(schema))
            )
            connection.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    checksum TEXT NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
                )
            """)
            rows = connection.execute(
                "SELECT version, checksum FROM schema_migrations ORDER BY version"
            ).fetchall()
            if [row["version"] for row in rows] != [
                version for version, _ in MIGRATIONS[:len(rows)]
            ]:
                raise RuntimeError("Unknown or nonsequential database migrations")
            existing = {row["version"]: row["checksum"] for row in rows}
            for version, migration_sql in MIGRATIONS:
                checksum = hashlib.sha256(migration_sql.encode()).hexdigest()
                if version in existing:
                    if existing[version] != checksum:
                        raise RuntimeError(f"Applied migration {version} was modified")
                    continue
                connection.execute(migration_sql, prepare=False)
                connection.execute(
                    sql.SQL("INSERT INTO {} (version, checksum) VALUES (%s, %s)")
                    .format(table(schema, "schema_migrations")),
                    (version, checksum),
                )
                applied.append(version)
    return applied


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply provider simulator migrations")
    parser.add_argument("--schema", default=PROVIDER_SCHEMA)
    args = parser.parse_args()
    applied = migrate(postgres_conninfo(), args.schema)
    print(f"Applied migrations: {applied}" if applied else "No pending migrations.")


if __name__ == "__main__":
    main()
