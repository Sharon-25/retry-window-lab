from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace
import json
import logging
import os
import subprocess
import sys
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from redis import Redis

from retry_window_lab.api import create_app
from retry_window_lab.config import postgres_conninfo,redis_url
from retry_window_lab.db import connect,table
from retry_window_lab.dispatcher import OutboxDispatcher
from retry_window_lab.domain import TaskDefinition
from retry_window_lab.events import LOGGER_NAME
from retry_window_lab.migrate import migrate
from retry_window_lab.ownership import Notification,OwnershipRepository
from retry_window_lab.provider import ProviderRepository
from retry_window_lab.provider_api import create_provider_app
from retry_window_lab.provider_client import Reply
from retry_window_lab.provider_migrate import migrate as provider_migrate
from retry_window_lab.recovery import ContextConflict,RecoveryRepository,RecoveryService
from retry_window_lab.storage import TaskRepository
from retry_window_lab.worker import Worker

pytestmark=pytest.mark.integration


@pytest.fixture
def environment():
    conninfo=postgres_conninfo()
    schema=f'rwl_test_{uuid4().hex}'
    provider_schema=f'rwl_test_{uuid4().hex}'
    stream=f'rwl:test:{uuid4().hex}'
    try:
        migrate(conninfo,schema)
        provider_migrate(conninfo,provider_schema)
        provider=ProviderRepository(conninfo,provider_schema)
        with TestClient(create_provider_app(provider)) as api,Redis.from_url(redis_url(),decode_responses=True) as client:
            client.ping()
            env=SimpleNamespace(conninfo=conninfo,schema=schema,provider_schema=provider_schema,provider=provider,
                                api=api,redis=client,stream=stream,store=TaskRepository(conninfo,schema),
                                ownership=OwnershipRepository(conninfo,schema),recovery=RecoveryRepository(conninfo,schema))
            class Client:
                def __init__(self,*args):
                    pass
                def reserve(self,body):
                    response=api.post('/reservations',json=body)
                    return Reply(response.status_code,response.json())
                def lookup(self,experiment_id,operation_id):
                    response=api.get(f'/experiments/{experiment_id}/operations/{operation_id}')
                    return Reply(response.status_code,response.json())
            env.client_factory=Client
            yield env
            client.delete(stream)
    finally:
        with connect(conninfo) as c:
            for name in (schema,provider_schema):
                c.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(name)))


def task(env,profile='authoritative',quantity=2):
    experiment=env.provider.create_experiment(profile)
    stored=env.store.submit(TaskDefinition('reserve_inventory',uuid4().hex,
        {'sku':'SKU-BOOK-001','quantity':quantity,'provider_profile':profile})).task
    env.recovery.bind(stored.task_id,experiment)
    return env.store.get(stored.task_id),experiment


def notification(env,task_id):
    with connect(env.conninfo) as c:
        event=c.execute(sql.SQL('SELECT * FROM {} WHERE task_id=%s ORDER BY delivery_generation DESC LIMIT 1')
                        .format(table(env.schema,'outbox')),(task_id,)).fetchone()
    return Notification(event['event_id'],task_id,event['delivery_generation'])


def acquire(env,stored):
    status,claim=env.ownership.claim(notification(env,stored.task_id),'reservation-worker')
    assert status=='claimed'
    return claim


def execute(env,stored):
    claim=acquire(env,stored)
    dispatch=env.recovery.begin_dispatch(claim)
    reply=env.client_factory().reserve(dispatch.body)
    return env.recovery.complete_mutation(claim,dispatch,reply),claim,dispatch


def row(env,task_id):
    with connect(env.conninfo) as c:
        return c.execute(sql.SQL('SELECT * FROM {} WHERE task_id=%s').format(table(env.schema,'tasks')),(task_id,)).fetchone()


def force_due(env,task_id,column='recovery_due_at'):
    assert column in {'recovery_due_at','retry_due_at','lease_until'}
    with connect(env.conninfo) as c:
        c.execute(sql.SQL("UPDATE {} SET {}=clock_timestamp()-interval '1 second' WHERE task_id=%s")
                  .format(table(env.schema,'tasks'),sql.Identifier(column)),(task_id,))


def reconcile(env,stored):
    force_due(env,stored.task_id)
    return RecoveryService(env.recovery,env.client_factory).run_once()


def count(env,name,task_id):
    with connect(env.conninfo) as c:
        return c.execute(sql.SQL('SELECT count(*) AS n FROM {} WHERE task_id=%s').format(table(env.schema,name)),(task_id,)).fetchone()['n']


def stock(env,experiment):
    with connect(env.conninfo) as c:
        return c.execute(sql.SQL("SELECT remaining FROM {} WHERE experiment_id=%s AND sku='SKU-BOOK-001'")
                         .format(table(env.provider_schema,'inventory')),(experiment,)).fetchone()['remaining']


def test_binding_is_immutable_and_republishes_ready_intent(environment):
    env=environment
    stored,experiment=task(env)
    assert stored.delivery_generation==2
    assert env.recovery.bind(stored.task_id,experiment) is False
    assert count(env,'outbox',stored.task_id)==2
    with pytest.raises(ContextConflict):
        env.recovery.bind(stored.task_id,uuid4())


def test_context_binding_rolls_back_with_outbox_failure(environment):
    env=environment
    stored=env.store.submit(TaskDefinition('reserve_inventory','bind-rollback',
        {'sku':'SKU-BOOK-001','quantity':2,'provider_profile':'authoritative'})).task
    with connect(env.conninfo) as c:
        c.execute(sql.SQL('''CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected outbox failure'; END; $$''').format(table(env.schema,'reject_outbox')))
        c.execute(sql.SQL('CREATE TRIGGER reject_outbox BEFORE INSERT ON {} FOR EACH ROW EXECUTE FUNCTION {}()')
                  .format(table(env.schema,'outbox'),table(env.schema,'reject_outbox')))
    with pytest.raises(psycopg.errors.RaiseException):
        env.recovery.bind(stored.task_id,uuid4())
    assert count(env,'provider_context',stored.task_id)==0
    assert row(env,stored.task_id)['delivery_generation']==1


def test_confirmed_reservation_persists_result_evidence_and_context(environment):
    env=environment
    stored,experiment=task(env)
    status,claim,dispatch=execute(env,stored)
    assert status=='SUCCEEDED'
    current=env.store.get(stored.task_id)
    assert current.as_dict()['result']['operation_id']==str(stored.operation_id)
    assert stock(env,experiment)==98
    assert count(env,'provider_dispatches',stored.task_id)==1
    assert count(env,'provider_observations',stored.task_id)==1
    audit=env.recovery.audit(stored.task_id)
    assert audit['context']['first_dispatch_at'] is not None
    assert audit['context']['protection_expires_at']==60
    assert audit['decisions'][-1]['reason']=='EFFECT_CONFIRMED'


def test_worker_uses_provider_handler_after_context_binding(environment,monkeypatch):
    env=environment
    import retry_window_lab.provider_client as module
    monkeypatch.setattr(module,'ProviderClient',env.client_factory)
    stored,experiment=task(env)
    OutboxDispatcher(env.conninfo,env.redis,env.schema,env.stream).dispatch_once()
    worker=Worker(env.ownership,env.redis,'worker',stream=env.stream)
    worker.ensure_group()
    assert worker.tick()['status']=='obsolete'
    assert worker.tick()['status']=='SUCCEEDED'
    assert stock(env,experiment)==98


def test_permanent_rejection_fails_without_retry(environment):
    env=environment
    stored,experiment=task(env,quantity=101)
    assert execute(env,stored)[0]=='FAILED'
    audit=env.recovery.audit(stored.task_id)
    assert audit['decisions'][-1]['reason']=='PERMANENT_REJECTION'
    assert env.recovery.schedule_due()==0 and stock(env,experiment)==100


def test_confirmed_no_effect_schedules_bounded_retry_and_new_generation(environment):
    env=environment
    stored,experiment=task(env)
    env.provider.inject(experiment,'REJECT_NO_EFFECT')
    status,claim,dispatch=execute(env,stored)
    assert status=='RETRY_WAIT'
    before=row(env,stored.task_id)
    assert before['retry_due_at'] is not None
    with connect(env.conninfo) as c:
        c.execute(sql.SQL("UPDATE {} SET retry_due_at=clock_timestamp()+interval '1 minute' WHERE task_id=%s")
                  .format(table(env.schema,'tasks')),(stored.task_id,))
    assert env.recovery.schedule_due()==0
    force_due(env,stored.task_id,'retry_due_at')
    assert env.recovery.schedule_due()==1
    assert row(env,stored.task_id)['delivery_generation']==3
    assert env.recovery.schedule_due()==0
    assert env.ownership.claim(notification(env,stored.task_id),'retry-worker')[0]=='claimed'
    assert stock(env,experiment)==100


def test_retry_deadline_survives_repository_restart(environment):
    env=environment
    stored,experiment=task(env)
    env.provider.inject(experiment,'REJECT_NO_EFFECT')
    execute(env,stored)
    due=row(env,stored.task_id)['retry_due_at']
    assert RecoveryRepository(env.conninfo,env.schema).audit(stored.task_id)['retry_due_at']==due


def test_three_settled_no_effect_attempts_exhaust_mutation_budget(environment):
    env=environment
    stored,experiment=task(env)
    for number in range(3):
        env.provider.inject(experiment,'REJECT_NO_EFFECT')
        status,_,_=execute(env,stored)
        assert status==('FAILED' if number==2 else 'RETRY_WAIT')
        if number<2:
            force_due(env,stored.task_id,'retry_due_at')
            env.recovery.schedule_due()
    current=row(env,stored.task_id)
    assert current['mutation_attempts']==3 and stock(env,experiment)==100
    assert env.recovery.audit(stored.task_id)['decisions'][-1]['reason']=='MUTATION_BUDGET_EXHAUSTED'


def test_response_loss_reconciles_success_without_second_post(environment):
    env=environment
    stored,experiment=task(env)
    env.provider.inject(experiment,'RESPONSE_LOST')
    assert execute(env,stored)[0]=='RECONCILING'
    assert reconcile(env,stored)=='SUCCEEDED'
    assert row(env,stored.task_id)['mutation_attempts']==1
    assert env.store.get(stored.task_id).as_dict()['result']['evidence_source']=='LOOKUP'
    assert stock(env,experiment)==98 and count(env,'provider_dispatches',stored.task_id)==1


def test_delayed_visibility_waits_then_confirms_without_new_mutation(environment):
    env=environment
    stored,experiment=task(env,'delayed')
    env.provider.inject(experiment,'RESPONSE_LOST')
    execute(env,stored)
    assert reconcile(env,stored)=='RECONCILING'
    env.provider.advance(experiment,10)
    assert reconcile(env,stored)=='SUCCEEDED'
    assert row(env,stored.task_id)['observation_count']==2
    assert count(env,'provider_dispatches',stored.task_id)==1


def test_unavailable_lookup_exhausts_observations_without_retry(environment):
    env=environment
    stored,experiment=task(env,'unavailable')
    env.provider.inject(experiment,'RESPONSE_LOST')
    execute(env,stored)
    env.provider.advance(experiment,120)
    for number in range(5):
        assert reconcile(env,stored)==('NEEDS_REVIEW' if number==4 else 'RECONCILING')
    assert row(env,stored.task_id)['observation_count']==5
    assert stock(env,experiment)==98 and count(env,'provider_dispatches',stored.task_id)==1
    assert env.recovery.audit(stored.task_id)['decisions'][-1]['reason']=='EVIDENCE_EXHAUSTED'


def test_negative_lookup_after_marker_before_post_never_retries(environment):
    env=environment
    stored,experiment=task(env)
    claim=acquire(env,stored)
    dispatch=env.recovery.begin_dispatch(claim)
    force_due(env,stored.task_id,'lease_until')
    assert env.ownership.expire()==1
    env.provider.advance(experiment,120)
    for _ in range(5):
        reconcile(env,stored)
    assert row(env,stored.task_id)['state']=='NEEDS_REVIEW'
    assert stock(env,experiment)==100 and count(env,'provider_dispatches',stored.task_id)==1
    # The old process may still POST after losing ownership; no new dispatch was authorized.
    response=env.client_factory().reserve(dispatch.body)
    assert stock(env,experiment)==98
    assert env.recovery.complete_mutation(claim,dispatch,response)=='stale_completion'
    assert row(env,stored.task_id)['state']=='NEEDS_REVIEW'


def test_expired_claim_before_marker_can_retry_but_old_owner_cannot_begin(environment):
    env=environment
    stored,experiment=task(env)
    claim=acquire(env,stored)
    force_due(env,stored.task_id,'lease_until')
    env.ownership.expire()
    assert row(env,stored.task_id)['state']=='RETRY_WAIT'
    assert env.recovery.begin_dispatch(claim) is None
    assert count(env,'provider_dispatches',stored.task_id)==0
    force_due(env,stored.task_id,'retry_due_at')
    env.recovery.schedule_due()
    assert execute(env,stored)[0]=='SUCCEEDED'
    assert stock(env,experiment)==98


def test_dispatch_marker_cannot_be_reused_for_a_second_post(environment):
    env=environment
    stored,_=task(env)
    claim=acquire(env,stored)
    assert env.recovery.begin_dispatch(claim) is not None
    assert env.recovery.begin_dispatch(claim) is None


def test_old_result_after_new_reconciliation_owner_is_rejected(environment):
    env=environment
    stored,experiment=task(env)
    claim=acquire(env,stored)
    dispatch=env.recovery.begin_dispatch(claim)
    reply=env.client_factory().reserve(dispatch.body)
    force_due(env,stored.task_id,'lease_until')
    env.ownership.expire()
    force_due(env,stored.task_id)
    job=env.recovery.claim_reconciliation()
    assert job[0].token>claim.token
    assert env.recovery.complete_mutation(claim,dispatch,reply)=='stale_completion'
    lookup=env.client_factory().lookup(experiment,str(stored.operation_id))
    assert env.recovery.complete_lookup(*job,lookup)=='SUCCEEDED'


def test_reconciliation_lease_loss_does_not_reset_observation_budget(environment):
    env=environment
    stored,experiment=task(env,'unavailable')
    env.provider.inject(experiment,'RESPONSE_LOST')
    execute(env,stored)
    for number in range(5):
        force_due(env,stored.task_id)
        job=env.recovery.claim_reconciliation()
        assert job is not None
        force_due(env,stored.task_id,'lease_until')
        env.ownership.expire()
    assert row(env,stored.task_id)['state']=='NEEDS_REVIEW'
    assert row(env,stored.task_id)['observation_count']==5


def test_competing_reconciliation_scanners_have_one_current_owner(environment):
    env=environment
    stored,experiment=task(env)
    env.provider.inject(experiment,'RESPONSE_LOST')
    execute(env,stored)
    force_due(env,stored.task_id)
    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs=list(pool.map(lambda _:env.recovery.claim_reconciliation(),range(4)))
    assert sum(job is not None for job in jobs)==1


def test_concurrent_schedulers_create_one_generation(environment):
    env=environment
    stored,experiment=task(env)
    env.provider.inject(experiment,'REJECT_NO_EFFECT')
    execute(env,stored)
    force_due(env,stored.task_id,'retry_due_at')
    with ThreadPoolExecutor(max_workers=4) as pool:
        counts=list(pool.map(lambda _:env.recovery.schedule_due(),range(4)))
    assert sum(counts)==1 and count(env,'outbox',stored.task_id)==3


def test_retry_outbox_failure_rolls_back_generation_and_state(environment):
    env=environment
    stored,experiment=task(env)
    env.provider.inject(experiment,'REJECT_NO_EFFECT')
    execute(env,stored)
    force_due(env,stored.task_id,'retry_due_at')
    with connect(env.conninfo) as c:
        c.execute(sql.SQL('''CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected retry publication failure'; END; $$''').format(table(env.schema,'reject_retry')))
        c.execute(sql.SQL('CREATE TRIGGER reject_retry BEFORE INSERT ON {} FOR EACH ROW EXECUTE FUNCTION {}()')
                  .format(table(env.schema,'outbox'),table(env.schema,'reject_retry')))
    with pytest.raises(psycopg.errors.RaiseException):
        env.recovery.schedule_due()
    current=row(env,stored.task_id)
    assert current['state']=='RETRY_WAIT' and current['delivery_generation']==2


def test_duplicate_provider_effect_is_reviewed(environment):
    env=environment
    stored,experiment=task(env)
    env.provider.inject(experiment,'RESPONSE_LOST')
    _,claim,dispatch=execute(env,stored)
    env.provider.advance(experiment,60)
    env.client_factory().reserve(dispatch.body)  # Unsafe external actor, outside queue policy.
    assert reconcile(env,stored)=='NEEDS_REVIEW'
    assert env.recovery.audit(stored.task_id)['decisions'][-1]['reason']=='DUPLICATE_EFFECT'


def test_late_original_does_not_trigger_an_unsafe_queue_retry(environment):
    env=environment
    stored,experiment=task(env)
    env.provider.inject(experiment,'DELAY_DELIVERY',130)
    assert execute(env,stored)[0]=='RECONCILING'
    env.provider.advance(experiment,60)
    assert reconcile(env,stored)=='RECONCILING'
    env.provider.advance(experiment,70)
    assert reconcile(env,stored)=='SUCCEEDED'
    assert stock(env,experiment)==98 and count(env,'provider_dispatches',stored.task_id)==1


def test_profile_mismatch_never_becomes_known_failure_or_retry(environment):
    env=environment
    stored=env.store.submit(TaskDefinition('reserve_inventory','wrong-profile',
        {'sku':'SKU-BOOK-001','quantity':2,'provider_profile':'delayed'})).task
    experiment=env.provider.create_experiment('authoritative')
    env.recovery.bind(stored.task_id,experiment)
    assert execute(env,stored)[0]=='RECONCILING'
    assert reconcile(env,stored)=='NEEDS_REVIEW'
    assert count(env,'provider_dispatches',stored.task_id)==1


def test_api_context_binding_and_recovery_status(environment):
    env=environment
    experiment=env.provider.create_experiment()
    with TestClient(create_app(env.store)) as api:
        body={'kind':'reserve_inventory','submission_key':'api-recovery',
              'payload':{'sku':'SKU-BOOK-001','quantity':2,'provider_profile':'authoritative'}}
        task_id=api.post('/tasks',json=body).json()['task']['task_id']
        path=f'/tasks/{task_id}/provider-context'
        assert api.post(path,json={'experiment_id':str(experiment)}).json()['created'] is True
        assert api.post(path,json={'experiment_id':str(experiment)}).json()['created'] is False
        assert api.post(path,json={'experiment_id':str(uuid4())}).status_code==409
        assert api.post(path,json={'experiment_id':str(experiment),'window_seconds':True}).status_code==422
        audit=api.get(f'/tasks/{task_id}/recovery')
        assert audit.status_code==200 and audit.json()['mutation_attempts']==0
        assert api.get(f'/tasks/{uuid4()}/recovery').status_code==404


def test_upgrade_from_phase6_preserves_checksum_tasks_and_migrations(environment,monkeypatch,legacy_checksum_task):
    import retry_window_lab.migrate as module
    env=environment
    schema=f'rwl_test_{uuid4().hex}'
    versions=module.MIGRATIONS
    try:
        monkeypatch.setattr(module,'MIGRATIONS',versions[:2])
        assert module.migrate(env.conninfo,schema)==[1,2]
        stored=legacy_checksum_task(env.conninfo,schema)
        monkeypatch.setattr(module,'MIGRATIONS',versions)
        assert module.migrate(env.conninfo,schema)==[version for version,_ in versions[2:]]
        assert TaskRepository(env.conninfo,schema).get(stored.task_id).as_dict()==stored.as_dict()
    finally:
        with connect(env.conninfo) as c:
            c.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))


def test_abrupt_process_exit_after_dispatch_marker_remains_uncertain(environment):
    env=environment
    stored,experiment=task(env)
    n=notification(env,stored.task_id)
    code='''
import os
from uuid import UUID
from retry_window_lab.ownership import Notification,OwnershipRepository
from retry_window_lab.recovery import RecoveryRepository
c=os.environ['RWL_TEST_CONN'];s=os.environ['RWL_TEST_SCHEMA']
n=Notification(UUID(os.environ['RWL_TEST_EVENT']),UUID(os.environ['RWL_TEST_TASK']),2)
status,claim=OwnershipRepository(c,s).claim(n,'crashing-provider-worker')
assert status=='claimed'
assert RecoveryRepository(c,s).begin_dispatch(claim) is not None
os._exit(17)
'''
    child=subprocess.run([sys.executable,'-c',code],env={**os.environ,
        'RWL_TEST_CONN':env.conninfo,'RWL_TEST_SCHEMA':env.schema,
        'RWL_TEST_TASK':str(stored.task_id),'RWL_TEST_EVENT':str(n.event_id)},
        capture_output=True,text=True,timeout=20)
    assert child.returncode==17,child.stderr
    force_due(env,stored.task_id,'lease_until')
    env.ownership.expire()
    assert row(env,stored.task_id)['state']=='RECONCILING'
    for _ in range(5):
        reconcile(env,stored)
    assert row(env,stored.task_id)['state']=='NEEDS_REVIEW'
    assert count(env,'provider_dispatches',stored.task_id)==1 and stock(env,experiment)==100


def test_monitoring_links_provider_history_and_committed_logs(environment, caplog):
    env = environment
    stored, experiment = task(env)
    env.provider.inject(experiment, "RESPONSE_LOST")
    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        state, claim, dispatch = execute(env, stored)
        assert state == "RECONCILING"
        assert reconcile(env, stored) == "SUCCEEDED"
        retry, retry_experiment = task(env)
        env.provider.inject(retry_experiment, "REJECT_NO_EFFECT")
        assert execute(env, retry)[0] == "RETRY_WAIT"

    with TestClient(create_app(env.store)) as api:
        snapshot = api.get("/monitoring/summary").json()
        assert snapshot["recovery"]["reconciliation_attempts"] == 1
        assert snapshot["recovery"]["retry_decisions"] == 1
        assert snapshot["states"]["RETRY_WAIT"] == 1
        history = api.get(f"/monitoring/tasks/{stored.task_id}").json()
    assert history["task"]["state"] == "SUCCEEDED"
    assert [row["purpose"] for row in history["attempts"]] == ["EXECUTION", "RECONCILIATION"]
    assert history["dispatches"][0]["dispatch_id"] == str(dispatch.dispatch_id)
    assert history["dispatches"][0]["attempt_id"] == str(claim.attempt_id)
    assert "request_body" not in history["dispatches"][0]
    assert [row["source"] for row in history["observations"]] == ["MUTATION", "LOOKUP"]
    assert all("body" not in row for row in history["observations"])
    assert [row["reason"] for row in history["decisions"]] == ["PROVIDER_UNKNOWN", "EFFECT_CONFIRMED"]
    assert {row["observation_id"] for row in history["decisions"]} == {
        row["observation_id"] for row in history["observations"]
    }
    events = [json.loads(record.getMessage()) for record in caplog.records
              if record.name == LOGGER_NAME]
    decisions = {entry["decision_id"]: entry for entry in events
                 if entry["event"] == "recovery_decision"}
    for decision in history["decisions"]:
        entry = decisions[decision["decision_id"]]
        assert entry["task_id"] == str(stored.task_id)
        assert entry["attempt_id"] == decision["attempt_id"]
        assert entry["observation_id"] == decision["observation_id"]


def test_rolled_back_recovery_decision_is_not_logged(environment, caplog):
    env = environment
    stored, experiment = task(env)
    env.provider.inject(experiment, "REJECT_NO_EFFECT")
    with connect(env.conninfo) as connection:
        connection.execute(sql.SQL("""
            CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'deferred decision failure'; END; $$
        """).format(table(env.schema, "fail_decision")))
        connection.execute(sql.SQL("""
            CREATE CONSTRAINT TRIGGER fail_decision
            AFTER INSERT ON {} DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW EXECUTE FUNCTION {}()
        """).format(table(env.schema, "recovery_decisions"), table(env.schema, "fail_decision")))
    with caplog.at_level(logging.INFO, logger=LOGGER_NAME), pytest.raises(psycopg.errors.RaiseException):
        execute(env, stored)
    assert env.store.get(stored.task_id).state.value == "RUNNING"
    assert count(env, "recovery_decisions", stored.task_id) == 0
    assert count(env, "provider_observations", stored.task_id) == 0
    assert all("recovery_decision" not in record.getMessage() for record in caplog.records
               if record.name == LOGGER_NAME)
