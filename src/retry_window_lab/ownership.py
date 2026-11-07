"""PostgreSQL ownership. All writers lock task first, then attempt."""
from dataclasses import dataclass
import json
from uuid import UUID, uuid4

from psycopg import sql
from psycopg.types.json import Jsonb

from retry_window_lab.db import DEFAULT_SCHEMA, connect, table, validate_schema
from retry_window_lab.domain import TaskDefinition, canonical_json
from retry_window_lab.events import emit_event


def bounded_int(value, minimum, maximum, name):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer in {minimum}..{maximum}")
    return value


def worker_identity(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 128:
        raise ValueError("worker_id must be nonblank and at most 128 characters")
    return value


@dataclass(frozen=True)
class Notification:
    event_id: UUID
    task_id: UUID
    generation: int

    @classmethod
    def parse(cls, fields):
        expected = {'protocol_version', 'event_id', 'task_id',
                    'delivery_generation', 'event_type'}
        if (type(fields) is not dict or set(fields) != expected
                or any(type(v) is not str for v in fields.values())
                or fields['protocol_version'] != '1'
                or fields['event_type'] != 'TASK_READY'):
            raise ValueError('Invalid ready notification')
        raw = fields['delivery_generation']
        if not raw.isascii() or not raw.isdecimal() or len(raw) > 19:
            raise ValueError('Invalid delivery generation')
        generation = bounded_int(int(raw), 1, 2**63-1, 'generation')
        return cls(UUID(fields['event_id']), UUID(fields['task_id']), generation)


@dataclass(frozen=True)
class Claim:
    task_id: UUID
    attempt_id: UUID
    token: int
    definition: TaskDefinition


class OwnershipRepository:
    def __init__(self, conninfo, schema=DEFAULT_SCHEMA):
        self.conninfo = conninfo
        self.schema = validate_schema(schema)

    def _table(self, name):
        return table(self.schema, name)

    def claim(self, notification, worker_id, lease_seconds=30):
        worker_identity(worker_id)
        bounded_int(lease_seconds, 3, 3600, 'lease_seconds')
        if not isinstance(notification, Notification):
            raise ValueError('Validated notification required')
        with connect(self.conninfo) as c, c.transaction():
            row = c.execute(sql.SQL('SELECT * FROM {} WHERE task_id=%s FOR UPDATE')
                            .format(self._table('tasks')), (notification.task_id,)).fetchone()
            if row is None:
                return 'obsolete', None
            event = c.execute(sql.SQL('''SELECT 1 FROM {} WHERE event_id=%s
                AND task_id=%s AND delivery_generation=%s AND event_type='TASK_READY' ''')
                .format(self._table('outbox')),
                (notification.event_id, notification.task_id, notification.generation)).fetchone()
            if not event or row['delivery_generation'] != notification.generation:
                return 'obsolete', None
            if row['state'] != 'QUEUED':
                return 'represented', None
            definition = TaskDefinition(row['kind'], row['submission_key'], row['payload'])
            if definition.fingerprint != row['fingerprint']:
                raise RuntimeError('Stored fingerprint mismatch')
            if definition.kind.value == 'reserve_inventory':
                context = c.execute(sql.SQL('SELECT 1 FROM {} WHERE task_id=%s')
                                    .format(self._table('provider_context')), (notification.task_id,)).fetchone()
                if not context:
                    return 'unsupported', None
                if row['mutation_attempts'] >= 3:
                    return 'unsupported', None
            attempt_id = uuid4()
            token = row['lease_token'] + 1
            c.execute(sql.SQL('''UPDATE {} SET state='RUNNING',lease_token=%s,
                active_attempt_id=%s,attempt_count=attempt_count+1,
                mutation_attempts=mutation_attempts+CASE WHEN kind='reserve_inventory' THEN 1 ELSE 0 END,
                lease_until=clock_timestamp()+(%s * interval '1 second'),
                updated_at=clock_timestamp() WHERE task_id=%s''').format(self._table('tasks')),
                (token, attempt_id, lease_seconds, notification.task_id))
            c.execute(sql.SQL('''INSERT INTO {}(attempt_id,task_id,number,lease_token,worker_id)
                VALUES(%s,%s,%s,%s,%s)''').format(self._table('attempts')),
                (attempt_id, notification.task_id, row['attempt_count']+1, token, worker_id))
            return 'claimed', Claim(notification.task_id, attempt_id, token, definition)

    def _current(self, c, claim):
        # Recheck expiration AFTER obtaining the row lock, using database time.
        row = c.execute(sql.SQL('SELECT * FROM {} WHERE task_id=%s FOR UPDATE')
                        .format(self._table('tasks')), (claim.task_id,)).fetchone()
        if (row is None or row['state'] not in ('RUNNING','RECONCILING')
                or row['lease_token'] != claim.token
                or row['active_attempt_id'] != claim.attempt_id):
            return False
        return c.execute(sql.SQL('SELECT lease_until > clock_timestamp() AS alive FROM {} WHERE task_id=%s')
                         .format(self._table('tasks')), (claim.task_id,)).fetchone()['alive'] is True

    def renew(self, claim, lease_seconds=30):
        bounded_int(lease_seconds, 3, 3600, 'lease_seconds')
        with connect(self.conninfo) as c, c.transaction():
            if not self._current(c, claim):
                return False
            c.execute(sql.SQL('''UPDATE {} SET lease_until=clock_timestamp()+(%s * interval '1 second'),
                updated_at=clock_timestamp() WHERE task_id=%s''').format(self._table('tasks')),
                (lease_seconds, claim.task_id))
            return True

    def finish(self, claim, outcome, result=None):
        outcomes = {'SUCCEEDED': ('SUCCEEDED', None),
                    'KNOWN_FAILURE': ('FAILED', 'KNOWN_PERMANENT_FAILURE'),
                    'UNKNOWN': ('NEEDS_REVIEW', 'UNEXPECTED_HANDLER_ERROR')}
        if outcome not in outcomes:
            raise ValueError('Invalid final outcome')
        if outcome == 'SUCCEEDED':
            if type(result) is not dict:
                raise ValueError('Success requires a JSON object')
            encoded = canonical_json(result)
            encoded.encode('utf-8')
            result = json.loads(encoded)
        elif result is not None:
            raise ValueError('Non-success result must be absent')
        state, code = outcomes[outcome]
        with connect(self.conninfo) as c, c.transaction():
            if not self._current(c, claim):
                return False
            cursor = c.execute(sql.SQL('''UPDATE {} SET outcome=%s,error_code=%s,finished_at=clock_timestamp()
                WHERE attempt_id=%s AND task_id=%s AND lease_token=%s AND outcome='RUNNING' ''')
                .format(self._table('attempts')), (outcome, code, claim.attempt_id, claim.task_id, claim.token))
            if cursor.rowcount != 1:
                raise RuntimeError('Current attempt missing')
            c.execute(sql.SQL('''UPDATE {} SET state=%s,result=%s,lease_until=NULL,
                active_attempt_id=NULL,updated_at=clock_timestamp() WHERE task_id=%s''')
                .format(self._table('tasks')), (state, None if result is None else Jsonb(result), claim.task_id))
            return True

    def expire(self, limit=100):
        bounded_int(limit, 1, 1000, 'limit')
        expired = []
        with connect(self.conninfo) as c, c.transaction():
            rows = c.execute(sql.SQL('''SELECT task_id FROM {} WHERE state IN ('RUNNING','RECONCILING')
                AND active_attempt_id IS NOT NULL AND lease_until <= clock_timestamp() ORDER BY lease_until,task_id
                LIMIT %s FOR UPDATE SKIP LOCKED''').format(self._table('tasks')), (limit,)).fetchall()
            for row in rows:
                task = c.execute(sql.SQL('SELECT * FROM {} WHERE task_id=%s')
                                 .format(self._table('tasks')), (row['task_id'],)).fetchone()
                if task['kind'] == 'reserve_inventory':
                    from retry_window_lab.recovery import RecoveryRepository
                    decision = RecoveryRepository(self.conninfo, self.schema).expire_locked(c, task)
                    if decision:
                        expired.append(('recovery_decision', decision))
                        continue
                # A lease expiration establishes uncertainty, never business failure.
                cursor = c.execute(sql.SQL('''UPDATE {} SET outcome='UNKNOWN',error_code='LEASE_EXPIRED',
                    finished_at=clock_timestamp() WHERE attempt_id=%s AND outcome='RUNNING' ''')
                    .format(self._table('attempts')), (task['active_attempt_id'],))
                if cursor.rowcount != 1:
                    raise RuntimeError('Expired attempt missing')
                c.execute(sql.SQL('''UPDATE {} SET state='NEEDS_REVIEW',lease_until=NULL,
                    active_attempt_id=NULL,updated_at=clock_timestamp() WHERE task_id=%s''')
                    .format(self._table('tasks')), (row['task_id'],))
                expired.append(('lease_expired', {
                    'task_id': task['task_id'], 'attempt_id': task['active_attempt_id'],
                    'lease_token': task['lease_token'], 'state': 'NEEDS_REVIEW',
                }))
        for event, fields in expired:
            emit_event(event, **fields)
        return len(expired)
