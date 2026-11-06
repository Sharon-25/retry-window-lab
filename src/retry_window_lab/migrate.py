import argparse
import hashlib

from psycopg import sql

from retry_window_lab.config import postgres_conninfo
from retry_window_lab.db import DEFAULT_SCHEMA, connect, table, validate_schema


# Applied migration text is immutable. Add later changes as new versions.
INITIAL_SQL = """
CREATE TABLE tasks (
    task_id UUID PRIMARY KEY,
    operation_id UUID NOT NULL UNIQUE,
    submission_key TEXT NOT NULL UNIQUE
        CHECK (char_length(submission_key) BETWEEN 1 AND 128
               AND btrim(submission_key) <> ''),
    kind TEXT NOT NULL CHECK (kind IN ('checksum', 'reserve_inventory')),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    fingerprint TEXT NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
    state TEXT NOT NULL DEFAULT 'QUEUED' CHECK (state IN (
        'QUEUED', 'RUNNING', 'RETRY_WAIT', 'RECONCILING',
        'NEEDS_REVIEW', 'SUCCEEDED', 'FAILED'
    )),
    delivery_generation BIGINT NOT NULL DEFAULT 1 CHECK (delivery_generation > 0),
    result JSONB CHECK (result IS NULL OR jsonb_typeof(result) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE outbox (
    event_id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    delivery_generation BIGINT NOT NULL CHECK (delivery_generation > 0),
    event_type TEXT NOT NULL CHECK (event_type = 'TASK_READY'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    published_at TIMESTAMPTZ,
    UNIQUE (task_id, delivery_generation, event_type)
);

CREATE INDEX outbox_unpublished_idx
    ON outbox (created_at, event_id) WHERE published_at IS NULL;
"""

WORKER_SQL = """
ALTER TABLE tasks ADD COLUMN lease_token BIGINT NOT NULL DEFAULT 0 CHECK (lease_token >= 0);
ALTER TABLE tasks ADD COLUMN lease_until TIMESTAMPTZ;
ALTER TABLE tasks ADD COLUMN active_attempt_id UUID;
ALTER TABLE tasks ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0);
CREATE TABLE attempts (
    attempt_id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    number INTEGER NOT NULL CHECK (number > 0),
    lease_token BIGINT NOT NULL CHECK (lease_token > 0),
    worker_id TEXT NOT NULL CHECK (char_length(worker_id) BETWEEN 1 AND 128),
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    outcome TEXT NOT NULL DEFAULT 'RUNNING'
        CHECK (outcome IN ('RUNNING','SUCCEEDED','KNOWN_FAILURE','UNKNOWN')),
    error_code TEXT CHECK (error_code IS NULL OR error_code IN (
        'KNOWN_PERMANENT_FAILURE','UNEXPECTED_HANDLER_ERROR','LEASE_EXPIRED'
    )),
    UNIQUE(task_id, number),
    UNIQUE(task_id, lease_token),
    CHECK ((outcome = 'RUNNING') = (finished_at IS NULL))
);
CREATE INDEX tasks_expired_lease_idx ON tasks(lease_until) WHERE state = 'RUNNING';
"""

RECOVERY_SQL = """
ALTER TABLE tasks ADD COLUMN mutation_attempts INTEGER NOT NULL DEFAULT 0 CHECK(mutation_attempts BETWEEN 0 AND 3);
ALTER TABLE tasks ADD COLUMN observation_count INTEGER NOT NULL DEFAULT 0 CHECK(observation_count BETWEEN 0 AND 5);
ALTER TABLE tasks ADD COLUMN retry_due_at TIMESTAMPTZ;
ALTER TABLE tasks ADD COLUMN recovery_due_at TIMESTAMPTZ;
ALTER TABLE attempts ADD COLUMN purpose TEXT NOT NULL DEFAULT 'EXECUTION' CHECK(purpose IN ('EXECUTION','RECONCILIATION'));
ALTER TABLE attempts DROP CONSTRAINT attempts_error_code_check;
ALTER TABLE attempts ADD CONSTRAINT attempts_error_code_check CHECK(error_code IS NULL OR error_code IN (
    'KNOWN_PERMANENT_FAILURE','UNEXPECTED_HANDLER_ERROR','LEASE_EXPIRED',
    'PROVIDER_UNKNOWN','LOOKUP_INCONCLUSIVE','PERMANENT_REJECTION','SETTLED_NO_EFFECT',
    'PROVIDER_CONFLICT','DUPLICATE_EFFECT','INVALID_EVIDENCE','EVIDENCE_EXHAUSTED'
));
CREATE TABLE provider_context (
    task_id UUID PRIMARY KEY REFERENCES tasks(task_id) ON DELETE CASCADE,
    experiment_id UUID NOT NULL,
    provider_url TEXT NOT NULL,
    profile TEXT NOT NULL CHECK(profile IN ('authoritative','delayed','unavailable')),
    profile_version INTEGER NOT NULL CHECK(profile_version=1),
    window_seconds INTEGER NOT NULL CHECK(window_seconds BETWEEN 1 AND 3600),
    first_dispatch_at TIMESTAMPTZ,
    accepted_provider_at BIGINT,
    protection_expires_at BIGINT
);
CREATE TABLE provider_dispatches (
    dispatch_id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    attempt_id UUID NOT NULL UNIQUE REFERENCES attempts(attempt_id),
    lease_token BIGINT NOT NULL,
    request_body JSONB NOT NULL CHECK(jsonb_typeof(request_body)='object'),
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    settled_no_effect BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE TABLE provider_observations (
    observation_id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    attempt_id UUID NOT NULL REFERENCES attempts(attempt_id),
    source TEXT NOT NULL CHECK(source IN ('MUTATION','LOOKUP')),
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    http_status INTEGER,
    error_code TEXT,
    body JSONB CHECK(body IS NULL OR jsonb_typeof(body)='object')
);
CREATE TABLE recovery_decisions (
    decision_id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    attempt_id UUID REFERENCES attempts(attempt_id),
    observation_id UUID REFERENCES provider_observations(observation_id),
    policy_version INTEGER NOT NULL DEFAULT 1 CHECK(policy_version=1),
    reason TEXT NOT NULL CHECK(reason IN (
        'EFFECT_CONFIRMED','PERMANENT_REJECTION','SETTLED_NO_EFFECT',
        'DISPATCH_NOT_STARTED','PROVIDER_UNKNOWN','LOOKUP_INCONCLUSIVE',
        'EVIDENCE_EXHAUSTED','MUTATION_BUDGET_EXHAUSTED','PROVIDER_CONFLICT',
        'DUPLICATE_EFFECT','INVALID_EVIDENCE','LEASE_EXPIRED'
    )),
    details JSONB NOT NULL DEFAULT '{}' CHECK(jsonb_typeof(details)='object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX tasks_recovery_due_idx ON tasks(recovery_due_at) WHERE state='RECONCILING';
CREATE INDEX tasks_retry_due_idx ON tasks(retry_due_at) WHERE state='RETRY_WAIT';
"""

MIGRATIONS = ((1, INITIAL_SQL), (2, WORKER_SQL), (3, RECOVERY_SQL))


def migrate(conninfo: str, schema: str = DEFAULT_SCHEMA) -> list[int]:
    schema = validate_schema(schema)
    applied = []
    with connect(conninfo) as connection:
        with connection.transaction():
            # Acquire before schema creation so competing migrators serialize.
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"retry-window-lab:migrate:{schema}",),
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
    parser = argparse.ArgumentParser(description="Apply versioned database migrations")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    args = parser.parse_args()
    applied = migrate(postgres_conninfo(), args.schema)
    print(f"Applied migrations: {applied}" if applied else "No pending migrations.")


if __name__ == "__main__":
    main()
