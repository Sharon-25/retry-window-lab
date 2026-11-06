"""One synchronous handler at a time per worker process."""
import argparse
import json
import socket
from threading import Event, Thread
from uuid import uuid4

import psycopg
from redis import Redis
from redis.exceptions import RedisError, ResponseError

from retry_window_lab.config import redis_url
from retry_window_lab.runtime_config import application_conninfo
from retry_window_lab.dispatcher import READY_STREAM
from retry_window_lab.domain import canonical_json
from retry_window_lab.local import KnownTaskFailure, checksum_handler
from retry_window_lab.ownership import (
    Notification, OwnershipRepository, bounded_int, worker_identity,
)

GROUP = 'rwl:workers'


class LeaseHeartbeat:
    def __init__(self, repository, claim, seconds):
        self.repository, self.claim, self.seconds = repository, claim, seconds
        self.stop = Event()
        self.lost = Event()
        self.thread = Thread(target=self._run, daemon=True, name='lease-heartbeat')

    def _run(self):
        while not self.stop.wait(self.seconds / 3):
            try:
                if self.repository.renew(self.claim, self.seconds):
                    continue
            except Exception:
                pass
            self.lost.set()
            return

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join()


def execute_claim(repository, claim, lease_seconds, handler=checksum_handler):
    result = None
    with LeaseHeartbeat(repository, claim, lease_seconds) as heartbeat:
        try:
            result = handler(claim.definition.payload)
            if type(result) is not dict:
                raise ValueError('Handler must return JSON object')
            canonical_json(result).encode('utf-8')
        except KnownTaskFailure:
            outcome, result = 'KNOWN_FAILURE', None
        except Exception:
            outcome, result = 'UNKNOWN', None
        else:
            outcome = 'SUCCEEDED'
    if heartbeat.lost.is_set():
        return 'ownership_lost'
    return outcome if repository.finish(claim, outcome, result) else 'stale_completion'


def execute_reservation(repository,claim,lease_seconds):
    from retry_window_lab.provider_client import ProviderClient
    from retry_window_lab.recovery import RecoveryRepository
    recovery=RecoveryRepository(repository.conninfo,repository.schema)
    with LeaseHeartbeat(repository,claim,lease_seconds) as heartbeat:
        dispatch=recovery.begin_dispatch(claim)
        if dispatch is None:
            return 'dispatch_not_started'
        reply=ProviderClient(dispatch.context['provider_url']).reserve(dispatch.body)
    if heartbeat.lost.is_set():
        return 'ownership_lost'
    return recovery.complete_mutation(claim,dispatch,reply)


class Worker:
    def __init__(self, repository, client, worker_id, stream=READY_STREAM,
                 group=GROUP, lease_seconds=30, reclaim_ms=60000):
        self.repository, self.client = repository, client
        self.worker_id = worker_identity(worker_id)
        self.stream, self.group = stream, group
        if any(not isinstance(s, str) or not s.strip() or len(s) > 128 for s in (stream, group)):
            raise ValueError('Stream and group must be nonblank and at most 128 characters')
        self.lease_seconds = bounded_int(lease_seconds, 3, 3600, 'lease_seconds')
        self.reclaim_ms = bounded_int(reclaim_ms, 1, 3600000, 'reclaim_ms')
        self.cursor = '0-0'

    def ensure_group(self):
        try:
            self.client.xgroup_create(self.stream, self.group, id='0-0', mkstream=True)
        except ResponseError as error:
            if not str(error).startswith('BUSYGROUP'):
                raise

    def tick(self):
        # Redis pending age is a transport concern, never permission to execute.
        reclaimed = self.client.xautoclaim(
            self.stream, self.group, self.worker_id, self.reclaim_ms,
            start_id=self.cursor, count=1,
        )
        self.cursor = reclaimed[0]
        entries = reclaimed[1]
        if not entries:
            batches = self.client.xreadgroup(self.group, self.worker_id,
                                             {self.stream: '>'}, count=1)
            entries = [] if not batches else batches[0][1]
        if not entries:
            return {'event': 'worker', 'status': 'idle'}
        message_id, fields = entries[0]
        try:
            notification = Notification.parse(fields)
        except (ValueError, TypeError):
            # Poison data is logged and acknowledged; never sent to a handler.
            self.client.xack(self.stream, self.group, message_id)
            return {'event': 'worker', 'status': 'invalid_notification', 'message_id': message_id}
        status, claim = self.repository.claim(notification, self.worker_id, self.lease_seconds)
        if status == 'unsupported':
            return {'event': 'worker', 'status': status, 'task_id': str(notification.task_id)}
        # A claim or durable representation is committed before acknowledgement.
        self.client.xack(self.stream, self.group, message_id)
        if claim is not None:
            status = (execute_reservation(self.repository, claim, self.lease_seconds)
                      if claim.definition.kind.value == 'reserve_inventory'
                      else execute_claim(self.repository, claim, self.lease_seconds))
        return {'event': 'worker', 'status': status, 'task_id': str(notification.task_id)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--worker-id', default=f'{socket.gethostname()}-{uuid4().hex[:12]}')
    parser.add_argument('--once', action='store_true', help='Handle at most one notification')
    parser.add_argument('--lease-seconds', type=int, default=30)
    args = parser.parse_args()
    repository = OwnershipRepository(application_conninfo())
    from retry_window_lab.recovery import RecoveryRepository,RecoveryService
    recovery_repository=RecoveryRepository(repository.conninfo)
    recovery_service=RecoveryService(recovery_repository,worker_id=args.worker_id)
    try:
        with Redis.from_url(redis_url(), decode_responses=True,
                            socket_connect_timeout=2, socket_timeout=2) as client:
            worker = Worker(repository, client, args.worker_id, lease_seconds=args.lease_seconds)
            worker.ensure_group()
            stop = Event()
            while True:
                try:
                    expired = repository.expire()
                    if expired:
                        print(json.dumps({'event': 'expired_attempts', 'count': expired}), flush=True)
                    scheduled=recovery_repository.schedule_due()
                    reconciled=recovery_service.run_once()
                    if scheduled or reconciled!='idle':
                        print(json.dumps({'event':'recovery','scheduled':scheduled,'status':reconciled}),flush=True)
                    report = worker.tick()
                    if args.once or report['status'] != 'idle':
                        print(json.dumps(report), flush=True)
                except (psycopg.Error, RedisError) as error:
                    print(json.dumps({'event': 'worker_error', 'type': type(error).__name__}), flush=True)
                    if args.once:
                        raise SystemExit(1)
                    stop.wait(1)
                    continue
                if args.once:
                    break
                if report['status'] == 'idle':
                    stop.wait(0.25)
    except KeyboardInterrupt:
        print(json.dumps({'event': 'worker_stopped'}), flush=True)


if __name__ == '__main__':
    main()
