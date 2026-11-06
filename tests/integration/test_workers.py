from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import os
import subprocess
import sys
from threading import Barrier, Event
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from redis import Redis

from retry_window_lab.config import postgres_conninfo, redis_url
from retry_window_lab.db import connect, table
from retry_window_lab.dispatcher import OutboxDispatcher
from retry_window_lab.domain import TaskDefinition
from retry_window_lab.migrate import migrate
from retry_window_lab.ownership import Notification, OwnershipRepository
from retry_window_lab.storage import TaskRepository
from retry_window_lab.worker import Worker, execute_claim

pytestmark = pytest.mark.integration


@pytest.fixture
def environment():
    conninfo = postgres_conninfo()
    schema = f'rwl_test_{uuid4().hex}'
    stream = f'rwl:test:{uuid4().hex}'
    with Redis.from_url(redis_url(), decode_responses=True,
                        socket_connect_timeout=2, socket_timeout=2) as client:
        client.ping()
        try:
            migrate(conninfo, schema)
            yield conninfo, schema, stream, client
        finally:
            client.delete(stream)
            with connect(conninfo) as c:
                c.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))


def setup_task(environment, kind='checksum'):
    conninfo, schema, stream, client = environment
    payload = {'text': 'abc'} if kind == 'checksum' else {
        'sku': 'SKU-BOOK-001', 'quantity': 1, 'provider_profile': 'authoritative'}
    task = TaskRepository(conninfo, schema).submit(TaskDefinition(kind, uuid4().hex, payload)).task
    with connect(conninfo) as c:
        event = c.execute(sql.SQL('SELECT * FROM {} WHERE task_id=%s').format(table(schema, 'outbox')),
                          (task.task_id,)).fetchone()
    notification = Notification(event['event_id'], task.task_id, 1)
    return task, notification, OwnershipRepository(conninfo, schema)


def snapshot(environment, task_id):
    conninfo, schema, *_ = environment
    with connect(conninfo) as c:
        task = c.execute(sql.SQL('SELECT * FROM {} WHERE task_id=%s').format(table(schema, 'tasks')),
                         (task_id,)).fetchone()
        attempts = c.execute(sql.SQL('SELECT * FROM {} WHERE task_id=%s ORDER BY number')
                             .format(table(schema, 'attempts')), (task_id,)).fetchall()
    return task, attempts


def force_expiry(environment, task_id):
    conninfo, schema, *_ = environment
    with connect(conninfo) as c:
        c.execute(sql.SQL("UPDATE {} SET lease_until=clock_timestamp()-interval '1 second' WHERE task_id=%s")
                  .format(table(schema, 'tasks')), (task_id,))


def test_claim_persists_attempt_and_ownership(environment):
    task, notification, repo = setup_task(environment)
    status, claim = repo.claim(notification, 'worker-a')
    assert status == 'claimed'
    row, attempts = snapshot(environment, task.task_id)
    assert row['state'] == 'RUNNING'
    assert row['lease_token'] == claim.token == 1
    assert row['active_attempt_id'] == claim.attempt_id
    assert row['attempt_count'] == 1
    assert attempts[0]['worker_id'] == 'worker-a'
    assert attempts[0]['outcome'] == 'RUNNING'
    assert attempts[0]['finished_at'] is None
    assert row['lease_until'] > attempts[0]['started_at']
    assert len({task.task_id, task.operation_id, claim.attempt_id}) == 3


def test_concurrent_claims_have_one_winner(environment):
    task, notification, repo = setup_task(environment)
    barrier = Barrier(8)
    def acquire(number):
        barrier.wait(timeout=10)
        return repo.claim(notification, f'worker-{number}')[0]
    with ThreadPoolExecutor(max_workers=8) as pool:
        statuses = list(pool.map(acquire, range(8)))
    assert statuses.count('claimed') == 1
    assert statuses.count('represented') == 7
    assert len(snapshot(environment, task.task_id)[1]) == 1


@pytest.mark.parametrize('change', ['event', 'generation', 'task'])
def test_invalid_reference_cannot_claim(environment, change):
    task, notification, repo = setup_task(environment)
    changes = {'event': {'event_id': uuid4()}, 'generation': {'generation': 2}, 'task': {'task_id': uuid4()}}
    assert repo.claim(replace(notification, **changes[change]), 'worker')[0] == 'obsolete'
    row, attempts = snapshot(environment, task.task_id)
    assert row['state'] == 'QUEUED'
    assert attempts == []


def test_claim_rolls_back_when_attempt_insert_fails(environment):
    task, notification, repo = setup_task(environment)
    conninfo, schema, *_ = environment
    with connect(conninfo) as c:
        c.execute(sql.SQL('''CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected attempt failure'; END; $$''').format(table(schema, 'reject_attempt')))
        c.execute(sql.SQL('CREATE TRIGGER reject_attempt BEFORE INSERT ON {} FOR EACH ROW EXECUTE FUNCTION {}()')
                  .format(table(schema, 'attempts'), table(schema, 'reject_attempt')))
    with pytest.raises(psycopg.errors.RaiseException):
        repo.claim(notification, 'worker')
    row, attempts = snapshot(environment, task.task_id)
    assert (row['state'], row['lease_token'], row['attempt_count']) == ('QUEUED', 0, 0)
    assert row['lease_until'] is None
    assert attempts == []


def test_renewal_extends_lease(environment):
    task, notification, repo = setup_task(environment)
    _, claim = repo.claim(notification, 'worker', 3)
    before = snapshot(environment, task.task_id)[0]['lease_until']
    assert repo.renew(claim, 30)
    assert snapshot(environment, task.task_id)[0]['lease_until'] > before


@pytest.mark.parametrize('change', ['token', 'attempt'])
def test_stale_owner_cannot_renew_or_finish(environment, change):
    task, notification, repo = setup_task(environment)
    _, claim = repo.claim(notification, 'worker')
    stale = replace(claim, **({'token': claim.token+1} if change == 'token' else {'attempt_id': uuid4()}))
    assert repo.renew(stale) is False
    assert repo.finish(stale, 'SUCCEEDED', {}) is False
    assert snapshot(environment, task.task_id)[0]['state'] == 'RUNNING'
    assert repo.finish(claim, 'SUCCEEDED', {'ok': True})


def test_expiration_blocks_completion_even_before_scanning(environment):
    task, notification, repo = setup_task(environment)
    _, claim = repo.claim(notification, 'worker')
    force_expiry(environment, task.task_id)
    assert repo.renew(claim) is False
    assert repo.finish(claim, 'SUCCEEDED', {}) is False
    assert repo.expire() == 1
    row, attempts = snapshot(environment, task.task_id)
    assert row['state'] == 'NEEDS_REVIEW'
    assert attempts[0]['outcome'] == 'UNKNOWN'
    assert attempts[0]['error_code'] == 'LEASE_EXPIRED'
    assert attempts[0]['finished_at'] is not None
    assert row['active_attempt_id'] is None and row['lease_until'] is None
    assert repo.expire() == 0
    assert repo.finish(claim, 'SUCCEEDED', {}) is False


def test_live_attempt_not_expired(environment):
    task, notification, repo = setup_task(environment)
    repo.claim(notification, 'worker')
    assert repo.expire() == 0
    assert snapshot(environment, task.task_id)[0]['state'] == 'RUNNING'


@pytest.mark.parametrize('outcome,state', [('SUCCEEDED', 'SUCCEEDED'), ('KNOWN_FAILURE', 'FAILED'), ('UNKNOWN', 'NEEDS_REVIEW')])
def test_completion_persists_task_and_attempt_atomically(environment, outcome, state):
    task, notification, repo = setup_task(environment)
    _, claim = repo.claim(notification, 'worker')
    assert repo.finish(claim, outcome, {'ok': True} if outcome == 'SUCCEEDED' else None)
    row, attempts = snapshot(environment, task.task_id)
    assert row['state'] == state and attempts[0]['outcome'] == outcome
    assert row['result'] == ({'ok': True} if outcome == 'SUCCEEDED' else None)
    assert repo.finish(claim, outcome, {'ok': True} if outcome == 'SUCCEEDED' else None) is False


def test_failed_task_update_rolls_back_attempt_completion(environment):
    task, notification, repo = setup_task(environment)
    _, claim = repo.claim(notification, 'worker')
    conninfo, schema, *_ = environment
    with connect(conninfo) as c:
        c.execute(sql.SQL('''CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.state='SUCCEEDED' THEN RAISE EXCEPTION 'injected finish failure'; END IF;
            RETURN NEW; END; $$''').format(table(schema, 'reject_finish')))
        c.execute(sql.SQL('CREATE TRIGGER reject_finish BEFORE UPDATE ON {} FOR EACH ROW EXECUTE FUNCTION {}()')
                  .format(table(schema, 'tasks'), table(schema, 'reject_finish')))
    with pytest.raises(psycopg.errors.RaiseException):
        repo.finish(claim, 'SUCCEEDED', {})
    row, attempts = snapshot(environment, task.task_id)
    assert row['state'] == attempts[0]['outcome'] == 'RUNNING'
    assert attempts[0]['finished_at'] is None


def test_duplicate_stream_entries_execute_only_one_attempt(environment):
    conninfo, schema, stream, client = environment
    task, notification, repo = setup_task(environment)
    OutboxDispatcher(conninfo, client, schema, stream).dispatch_once()
    fields = client.xrange(stream)[0][1]
    client.xadd(stream, fields)
    worker = Worker(repo, client, 'worker', stream=stream)
    worker.ensure_group()
    assert worker.tick()['status'] == 'SUCCEEDED'
    assert worker.tick()['status'] == 'represented'
    row, attempts = snapshot(environment, task.task_id)
    assert row['result']['sha256'] == 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'
    assert len(attempts) == 1
    assert client.xpending(stream, worker.group)['pending'] == 0


def test_crashed_consumer_pending_entry_can_be_reclaimed(environment):
    conninfo, schema, stream, client = environment
    task, notification, repo = setup_task(environment)
    OutboxDispatcher(conninfo, client, schema, stream).dispatch_once()
    worker = Worker(repo, client, 'replacement', stream=stream, reclaim_ms=1)
    worker.ensure_group()
    delivered = client.xreadgroup(worker.group, 'crashed', {stream: '>'}, count=1)
    message_id = delivered[0][1][0][0]
    # Set Redis idle time explicitly; avoid timing-sensitive sleeps.
    client.xclaim(stream, worker.group, 'crashed', 0, [message_id], idle=1000)
    assert worker.tick()['status'] == 'SUCCEEDED'
    assert len(snapshot(environment, task.task_id)[1]) == 1


def test_notification_ack_does_not_mean_task_succeeded(environment):
    conninfo, schema, stream, client = environment
    task, notification, repo = setup_task(environment)
    OutboxDispatcher(conninfo, client, schema, stream).dispatch_once()
    worker = Worker(repo, client, 'replacement', stream=stream, reclaim_ms=1)
    worker.ensure_group()
    delivered = client.xreadgroup(worker.group, 'crashed', {stream: '>'}, count=1)
    repo.claim(notification, 'crashed')
    message_id = delivered[0][1][0][0]
    client.xclaim(stream, worker.group, 'crashed', 0, [message_id], idle=1000)
    assert worker.tick()['status'] == 'represented'
    assert client.xpending(stream, worker.group)['pending'] == 0
    assert snapshot(environment, task.task_id)[0]['state'] == 'RUNNING'
    force_expiry(environment, task.task_id)
    assert repo.expire() == 1
    assert snapshot(environment, task.task_id)[0]['state'] == 'NEEDS_REVIEW'


def test_reservation_has_no_handler_and_is_not_claimed(environment):
    task, notification, repo = setup_task(environment, 'reserve_inventory')
    assert repo.claim(notification, 'worker')[0] == 'unsupported'
    row, attempts = snapshot(environment, task.task_id)
    assert row['state'] == 'QUEUED' and attempts == []


def test_expiry_scanners_do_not_double_finalize(environment):
    task, notification, repo = setup_task(environment)
    repo.claim(notification, 'worker')
    force_expiry(environment, task.task_id)
    with ThreadPoolExecutor(max_workers=2) as pool:
        counts = list(pool.map(lambda _: repo.expire(), range(2)))
    assert sum(counts) == 1


def test_heartbeat_renews_while_handler_is_active(environment):
    task, notification, repo = setup_task(environment)
    _, claim = repo.claim(notification, 'worker', 3)
    renewed = Event()
    original = repo.renew
    def renew(*args):
        result = original(*args)
        if result:
            renewed.set()
        return result
    repo.renew = renew
    def handler(payload):
        assert renewed.wait(8), 'Heartbeat did not renew ownership'
        return {'renewed': True}
    assert execute_claim(repo, claim, 3, handler) == 'SUCCEEDED'
    assert snapshot(environment, task.task_id)[0]['result'] == {'renewed': True}


def child_claim(environment, notification, hard_exit=False):
    conninfo, schema, *_ = environment
    code = '''
import json, os
from retry_window_lab.ownership import Notification, OwnershipRepository
from uuid import UUID
n = Notification(UUID(os.environ['RWL_EVENT']), UUID(os.environ['RWL_TASK']), 1)
status, claim = OwnershipRepository(os.environ['RWL_CONN'], os.environ['RWL_SCHEMA']).claim(n, str(os.getpid()))
print(json.dumps({'status': status}), flush=True)
if os.environ['RWL_CRASH'] == '1':
    os._exit(17)
'''
    env = {**os.environ, 'RWL_CONN': conninfo, 'RWL_SCHEMA': schema,
           'RWL_TASK': str(notification.task_id), 'RWL_EVENT': str(notification.event_id),
           'RWL_CRASH': '1' if hard_exit else '0'}
    return subprocess.run([sys.executable, '-c', code], env=env, text=True,
                          capture_output=True, timeout=25)


def test_abrupt_process_exit_leaves_durable_attempt_for_recovery(environment):
    task, notification, repo = setup_task(environment)
    child = child_claim(environment, notification, hard_exit=True)
    assert child.returncode == 17, child.stderr
    assert json.loads(child.stdout)['status'] == 'claimed'
    row, attempts = snapshot(environment, task.task_id)
    assert row['state'] == attempts[0]['outcome'] == 'RUNNING'
    force_expiry(environment, task.task_id)
    assert repo.expire() == 1
    assert snapshot(environment, task.task_id)[0]['state'] == 'NEEDS_REVIEW'


def test_independent_processes_compete_for_one_claim(environment):
    task, notification, repo = setup_task(environment)
    with ThreadPoolExecutor(max_workers=4) as pool:
        children = list(pool.map(lambda _: child_claim(environment, notification), range(4)))
    for child in children:
        assert child.returncode == 0, child.stderr
    assert sum(json.loads(child.stdout)['status'] == 'claimed' for child in children) == 1
    assert len(snapshot(environment, task.task_id)[1]) == 1


def test_upgrade_from_phase4_preserves_tasks_and_outbox(environment, monkeypatch):
    import retry_window_lab.migrate as module
    conninfo, schema, *_ = environment
    old_schema = f'rwl_test_{uuid4().hex}'
    versions = module.MIGRATIONS
    try:
        monkeypatch.setattr(module, 'MIGRATIONS', versions[:1])
        assert module.migrate(conninfo, old_schema) == [1]
        task = TaskRepository(conninfo, old_schema).submit(TaskDefinition('checksum', 'upgrade', {'text': 'abc'})).task
        monkeypatch.setattr(module, 'MIGRATIONS', versions)
        assert module.migrate(conninfo, old_schema) == [2]
        assert module.migrate(conninfo, old_schema) == []
        assert TaskRepository(conninfo, old_schema).get(task.task_id).as_dict() == task.as_dict()
        with connect(conninfo) as c:
            assert c.execute(sql.SQL('SELECT count(*) AS n FROM {}').format(table(old_schema, 'outbox'))).fetchone()['n'] == 1
    finally:
        with connect(conninfo) as c:
            c.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(old_schema)))


def test_multiple_worker_processes_complete_distinct_tasks(environment):
    conninfo, schema, stream, client = environment
    tasks = [setup_task(environment)[0] for _ in range(8)]
    OutboxDispatcher(conninfo, client, schema, stream).dispatch_once()
    code = '''
import json, os
from redis import Redis
from retry_window_lab.worker import Worker
from retry_window_lab.ownership import OwnershipRepository
with Redis.from_url(os.environ['REDIS_URL'], decode_responses=True, socket_timeout=2) as client:
    worker = Worker(OwnershipRepository(os.environ['RWL_CONN'], os.environ['RWL_SCHEMA']),
                    client, str(os.getpid()), stream=os.environ['RWL_STREAM'])
    worker.ensure_group()
    print(json.dumps([worker.tick()['status'] for _ in range(2)]), flush=True)
'''
    env = {**os.environ, 'RWL_CONN': conninfo, 'RWL_SCHEMA': schema, 'RWL_STREAM': stream,
           'REDIS_URL': redis_url()}
    def run(_):
        return subprocess.run([sys.executable, '-c', code], env=env,
                              capture_output=True, text=True, timeout=30)
    with ThreadPoolExecutor(max_workers=4) as pool:
        children = list(pool.map(run, range(4)))
    for child in children:
        assert child.returncode == 0, child.stderr
        assert json.loads(child.stdout) == ['SUCCEEDED', 'SUCCEEDED']
    for task in tasks:
        row, attempts = snapshot(environment, task.task_id)
        assert row['state'] == 'SUCCEEDED' and len(attempts) == 1
    assert client.xpending(stream, 'rwl:workers')['pending'] == 0
