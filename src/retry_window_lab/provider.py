"""Finite-window reservation simulator. Provider time is an integer experiment clock."""
from dataclasses import dataclass
import hashlib
from uuid import UUID, uuid4

from psycopg import sql

from retry_window_lab.db import connect, table, validate_schema
from retry_window_lab.domain import KNOWN_SKUS, PROVIDER_PROFILES, canonical_json
from retry_window_lab.ownership import bounded_int
from retry_window_lab.provider_migrate import PROVIDER_SCHEMA

FAULTS = {'NONE', 'RESPONSE_LOST', 'REJECT_NO_EFFECT', 'DELAY_DELIVERY'}


class ExperimentMissing(LookupError):
    pass


class LookupUnavailable(Exception):
    pass


@dataclass(frozen=True)
class ReservationRequest:
    experiment_id: UUID
    operation_id: UUID
    idempotency_key: str
    sku: str
    quantity: int

    def __post_init__(self):
        if not isinstance(self.experiment_id, UUID) or not isinstance(self.operation_id, UUID):
            raise ValueError('Experiment and operation must be UUIDs')
        if (type(self.idempotency_key) is not str or not self.idempotency_key.strip()
                or len(self.idempotency_key) > 128 or '\x00' in self.idempotency_key):
            raise ValueError('Key must be nonblank and at most 128 characters')
        if type(self.sku) is not str or self.sku not in KNOWN_SKUS:
            raise ValueError('Unknown SKU')
        bounded_int(self.quantity, 1, 2**31-1, 'quantity')
        canonical_json({'key': self.idempotency_key}).encode('utf-8')

    @property
    def fingerprint(self):
        body = {'schema_version': 1, 'sku': self.sku, 'quantity': self.quantity}
        return hashlib.sha256(canonical_json(body).encode('utf-8')).hexdigest()


class ProviderRepository:
    def __init__(self, conninfo, schema=PROVIDER_SCHEMA):
        self.conninfo, self.schema = conninfo, validate_schema(schema)

    def _table(self, name):
        return table(self.schema, name)

    def _experiment(self, c, experiment_id):
        row = c.execute(sql.SQL('SELECT * FROM {} WHERE experiment_id=%s FOR UPDATE')
                        .format(self._table('experiments')), (experiment_id,)).fetchone()
        if row is None:
            raise ExperimentMissing('Unknown experiment')
        return row

    def create_experiment(self, profile='authoritative', window_seconds=60,
                          visibility_delay=10, experiment_id=None):
        if profile not in PROVIDER_PROFILES:
            raise ValueError('Unknown provider profile')
        bounded_int(window_seconds, 1, 3600, 'window_seconds')
        bounded_int(visibility_delay, 0, 3600, 'visibility_delay')
        experiment_id = uuid4() if experiment_id is None else experiment_id
        if not isinstance(experiment_id, UUID):
            raise ValueError('Experiment must be a UUID')
        with connect(self.conninfo) as c, c.transaction():
            c.execute(sql.SQL('''INSERT INTO {}(experiment_id,profile,window_seconds,visibility_delay)
                VALUES(%s,%s,%s,%s)''').format(self._table('experiments')),
                (experiment_id, profile, window_seconds, visibility_delay))
            for sku in sorted(KNOWN_SKUS):
                c.execute(sql.SQL('INSERT INTO {}(experiment_id,sku,remaining) VALUES(%s,%s,100)')
                          .format(self._table('inventory')), (experiment_id, sku))
        return experiment_id

    def inject(self, experiment_id, fault, delay_seconds=0):
        if fault not in FAULTS:
            raise ValueError('Unknown fault')
        bounded_int(delay_seconds, 0, 3600, 'delay_seconds')
        if (fault == 'DELAY_DELIVERY') != (delay_seconds > 0):
            raise ValueError('Only DELAY_DELIVERY requires a positive delay')
        with connect(self.conninfo) as c, c.transaction():
            self._experiment(c, experiment_id)
            c.execute(sql.SQL('UPDATE {} SET next_fault=%s,next_delay=%s WHERE experiment_id=%s')
                      .format(self._table('experiments')), (fault, delay_seconds, experiment_id))

    def _response(self, request, experiment, lost=False):
        return {
            'request_id': str(request['request_id']),
            'experiment_id': str(request['experiment_id']),
            'operation_id': str(request['operation_id']),
            'status': request['status'], 'error_code': request['error_code'],
            'reservation_id': None if request['reservation_id'] is None else str(request['reservation_id']),
            'fingerprint': request['fingerprint'], 'sku': request['sku'], 'quantity': request['quantity'],
            'accepted_at': request['accepted_at'], 'protection_expires_at': request['expires_at'],
            'provider_now': experiment['now_seconds'], 'profile': experiment['profile'],
            'profile_version': experiment['profile_version'], 'window_seconds': experiment['window_seconds'],
            'reused': request['reused'], 'response_lost': lost,
        }

    def _settle(self, c, request, experiment):
        now = experiment['now_seconds']
        identity = (request['experiment_id'], request['operation_id'], request['idempotency_key'])
        protected = c.execute(sql.SQL('''SELECT * FROM {} WHERE experiment_id=%s
            AND operation_id=%s AND idempotency_key=%s''').format(self._table('protection_keys')),
            identity).fetchone()
        reservation = None
        reused = False
        code = None
        accepted_at = expires_at = None
        if protected is not None and protected['expires_at'] > now:
            if protected['fingerprint'] != request['fingerprint']:
                status, code = 'CONFLICT', 'KEY_CONFLICT'
            else:
                status, reused = 'SUCCEEDED', True
                reservation = protected['reservation_id']
                accepted_at, expires_at = protected['accepted_at'], protected['expires_at']
        else:
            stock = c.execute(sql.SQL('''UPDATE {} SET remaining=remaining-%s
                WHERE experiment_id=%s AND sku=%s AND remaining >= %s RETURNING remaining''')
                .format(self._table('inventory')),
                (request['quantity'], request['experiment_id'], request['sku'], request['quantity'])).fetchone()
            if stock is None:
                status, code = 'REJECTED', 'INSUFFICIENT_STOCK'
            else:
                status = 'SUCCEEDED'
                reservation, accepted_at, expires_at = uuid4(), now, now + experiment['window_seconds']
                visible_at = now + (experiment['visibility_delay'] if experiment['profile'] == 'delayed' else 0)
                c.execute(sql.SQL('''INSERT INTO {}(reservation_id,experiment_id,operation_id,
                    idempotency_key,fingerprint,sku,quantity,accepted_at,visible_at)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)''').format(self._table('reservations')),
                    (reservation, *identity, request['fingerprint'], request['sku'], request['quantity'], now, visible_at))
                c.execute(sql.SQL('''INSERT INTO {}(experiment_id,operation_id,idempotency_key,
                    fingerprint,reservation_id,accepted_at,expires_at) VALUES(%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(experiment_id,operation_id,idempotency_key) DO UPDATE SET
                    fingerprint=excluded.fingerprint,reservation_id=excluded.reservation_id,
                    accepted_at=excluded.accepted_at,expires_at=excluded.expires_at''')
                    .format(self._table('protection_keys')),
                    (*identity, request['fingerprint'], reservation, accepted_at, expires_at))
        return c.execute(sql.SQL('''UPDATE {} SET status=%s,error_code=%s,reservation_id=%s,
            accepted_at=%s,expires_at=%s,reused=%s WHERE request_id=%s RETURNING *''')
            .format(self._table('requests')),
            (status, code, reservation, accepted_at, expires_at, reused, request['request_id'])).fetchone()

    def reserve(self, request):
        if not isinstance(request, ReservationRequest):
            raise ValueError('Validated reservation request required')
        with connect(self.conninfo) as c, c.transaction():
            experiment = self._experiment(c, request.experiment_id)
            fault, delay = experiment['next_fault'], experiment['next_delay']
            c.execute(sql.SQL("UPDATE {} SET next_fault='NONE',next_delay=0 WHERE experiment_id=%s")
                      .format(self._table('experiments')), (request.experiment_id,))
            row = c.execute(sql.SQL('''INSERT INTO {}(request_id,experiment_id,operation_id,
                idempotency_key,fingerprint,sku,quantity,due_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *''')
                .format(self._table('requests')),
                (uuid4(), request.experiment_id, request.operation_id, request.idempotency_key,
                 request.fingerprint, request.sku, request.quantity, experiment['now_seconds']+delay)).fetchone()
            if fault == 'REJECT_NO_EFFECT':
                row = c.execute(sql.SQL("UPDATE {} SET status='REJECTED',error_code='SETTLED_NO_EFFECT' WHERE request_id=%s RETURNING *")
                                .format(self._table('requests')), (row['request_id'],)).fetchone()
            elif fault != 'DELAY_DELIVERY':
                row = self._settle(c, row, experiment)
            response = self._response(row, experiment, lost=fault == 'RESPONSE_LOST')
        # Commit happens before a simulated response-loss error is surfaced by HTTP.
        return response

    def advance(self, experiment_id, seconds):
        bounded_int(seconds, 0, 3600, 'seconds')
        with connect(self.conninfo) as c, c.transaction():
            experiment = self._experiment(c, experiment_id)
            now = experiment['now_seconds'] + seconds
            if now > 10**12:
                raise ValueError('Experiment clock limit exceeded')
            experiment = c.execute(sql.SQL('UPDATE {} SET now_seconds=%s WHERE experiment_id=%s RETURNING *')
                                   .format(self._table('experiments')), (now, experiment_id)).fetchone()
            pending = c.execute(sql.SQL("SELECT * FROM {} WHERE experiment_id=%s AND status='PROCESSING' AND due_at<=%s ORDER BY due_at,request_id")
                                .format(self._table('requests')), (experiment_id, now)).fetchall()
            for request in pending:
                self._settle(c, request, experiment)
            return {'experiment_id': str(experiment_id), 'provider_now': now, 'settled_requests': len(pending)}

    def lookup(self, experiment_id, operation_id):
        with connect(self.conninfo) as c, c.transaction():
            experiment = self._experiment(c, experiment_id)
            if experiment['profile'] == 'unavailable':
                raise LookupUnavailable()
            reservations = c.execute(sql.SQL('''SELECT reservation_id,operation_id,fingerprint,sku,quantity,
                accepted_at,visible_at FROM {} WHERE experiment_id=%s AND operation_id=%s
                AND visible_at<=%s ORDER BY accepted_at,reservation_id''').format(self._table('reservations')),
                (experiment_id, operation_id, experiment['now_seconds'])).fetchall()
            receipts = []
            if experiment['profile'] == 'authoritative':
                receipts = c.execute(sql.SQL('''SELECT request_id,status,error_code FROM {} WHERE
                    experiment_id=%s AND operation_id=%s ORDER BY due_at,request_id''').format(self._table('requests')),
                    (experiment_id, operation_id)).fetchall()
            return {'experiment_id': str(experiment_id), 'operation_id': str(operation_id),
                    'provider_now': experiment['now_seconds'], 'profile': experiment['profile'],
                    'profile_version': experiment['profile_version'],
                    'status': 'FOUND' if reservations else 'NOT_FOUND',
                    'reservations': [{k: str(v) if isinstance(v, UUID) else v for k,v in row.items()} for row in reservations],
                    'requests': [{k: str(v) if isinstance(v, UUID) else v for k,v in row.items()} for row in receipts],
                    'absence_authorizes_retry': False}
