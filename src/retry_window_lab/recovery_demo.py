"""Operator demo using real provider HTTP, restricted queue credentials and isolated queue storage."""
import argparse
import json
import os
from threading import Event
import time
from uuid import uuid4

from psycopg import sql
from redis import Redis

from retry_window_lab.config import postgres_conninfo,redis_url
from retry_window_lab.db import connect,table,validate_schema
from retry_window_lab.dispatcher import OutboxDispatcher
from retry_window_lab.domain import TaskDefinition
from retry_window_lab.migrate import migrate
from retry_window_lab.ownership import OwnershipRepository
from retry_window_lab.provider import ProviderRepository
from retry_window_lab.provider_client import provider_url
from retry_window_lab.recovery import RecoveryRepository,RecoveryService
from retry_window_lab.runtime_config import application_conninfo
from retry_window_lab.storage import TaskRepository
from retry_window_lab.worker import Worker


RUNTIME_TABLES=('tasks','attempts','outbox','provider_context','provider_dispatches',
                'provider_observations','recovery_decisions')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url')
    args=parser.parse_args()
    admin=postgres_conninfo()
    base=provider_url(args.base_url or os.environ.get('RWL_PROVIDER_URL','http://127.0.0.1:8001'))
    app=application_conninfo()
    schema=f'rwl_demo_{uuid4().hex}'
    stream=f'rwl:demo:{uuid4().hex}'
    operator=ProviderRepository(admin)
    stop=Event()
    try:
        migrate(admin,schema)
        role=validate_schema(os.environ['RWL_APP_USER'])
        with connect(admin) as c,c.transaction():
            c.execute(sql.SQL('GRANT USAGE ON SCHEMA {} TO {}').format(sql.Identifier(schema),sql.Identifier(role)))
            for name in RUNTIME_TABLES:
                c.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE ON {} TO {}').format(table(schema,name),sql.Identifier(role)))
        store=TaskRepository(app,schema)
        ownership=OwnershipRepository(app,schema)
        recovery=RecoveryRepository(app,schema)
        service=RecoveryService(recovery)
        with Redis.from_url(redis_url(),decode_responses=True,socket_connect_timeout=2,socket_timeout=2) as client:
            try:
                worker=Worker(ownership,client,'recovery-demo-worker',stream=stream)
                worker.ensure_group()
                dispatcher=OutboxDispatcher(app,client,schema,stream)

                def submit(profile='authoritative',fault=None,delay=0):
                    experiment=operator.create_experiment(profile)
                    if fault:
                        operator.inject(experiment,fault,delay)
                    task=store.submit(TaskDefinition('reserve_inventory',uuid4().hex,
                        {'sku':'SKU-BOOK-001','quantity':2,'provider_profile':profile})).task
                    recovery.bind(task.task_id,experiment,base)
                    return task,experiment

                def until(task,predicate,timeout=25):
                    deadline=time.monotonic()+timeout
                    while True:
                        ownership.expire()
                        recovery.schedule_due()
                        dispatcher.dispatch_once()
                        worker.tick()
                        service.run_once()
                        audit=recovery.audit(task.task_id)
                        if predicate(audit):
                            return audit
                        if time.monotonic()>=deadline:
                            raise RuntimeError(f"Demo timed out in {audit['state']}")
                        stop.wait(.05)

                def dispatch_count(task):
                    with connect(app) as c:
                        return c.execute(sql.SQL('SELECT count(*) AS n FROM {} WHERE task_id=%s')
                            .format(table(schema,'provider_dispatches')),(task.task_id,)).fetchone()['n']

                def one_effect(task,experiment):
                    # Independent operator ground truth; never used by queue recovery.
                    with connect(admin) as c:
                        total=c.execute('SELECT count(*) AS n FROM rwl_provider.reservations WHERE experiment_id=%s AND operation_id=%s',
                                        (experiment,task.operation_id)).fetchone()['n']
                    assert total==1

                normal,exp=submit()
                until(normal,lambda a:a['state']=='SUCCEEDED')
                assert dispatch_count(normal)==1
                one_effect(normal,exp)

                lost,exp=submit(fault='RESPONSE_LOST')
                until(lost,lambda a:a['state']=='RECONCILING')
                operator.advance(exp,120)
                until(lost,lambda a:a['state']=='SUCCEEDED')
                assert dispatch_count(lost)==1
                one_effect(lost,exp)

                delayed,exp=submit('delayed','RESPONSE_LOST')
                until(delayed,lambda a:a['observation_count']>=1)
                assert store.get(delayed.task_id).state.value=='RECONCILING'
                operator.advance(exp,10)
                until(delayed,lambda a:a['state']=='SUCCEEDED')
                assert dispatch_count(delayed)==1
                one_effect(delayed,exp)

                retry,exp=submit(fault='REJECT_NO_EFFECT')
                until(retry,lambda a:a['state']=='SUCCEEDED')
                assert dispatch_count(retry)==2
                one_effect(retry,exp)

                unavailable,exp=submit('unavailable','RESPONSE_LOST')
                until(unavailable,lambda a:a['state']=='RECONCILING')
                operator.advance(exp,120)
                audit=until(unavailable,lambda a:a['state']=='NEEDS_REVIEW')
                assert audit['observation_count']==5 and dispatch_count(unavailable)==1
                one_effect(unavailable,exp)

                late,exp=submit(fault='DELAY_DELIVERY',delay=130)
                until(late,lambda a:a['state']=='RECONCILING')
                operator.advance(exp,60)
                until(late,lambda a:a['observation_count']>=1)
                assert dispatch_count(late)==1
                operator.advance(exp,70)
                until(late,lambda a:a['state']=='SUCCEEDED')
                assert dispatch_count(late)==1
                one_effect(late,exp)

                print(json.dumps({
                    'normal_reservation_succeeded':True,
                    'lost_response_reconciled_after_expiry_without_retry':True,
                    'delayed_visibility_reconciled_without_retry':True,
                    'confirmed_no_effect_retried_safely':True,
                    'unavailable_lookup_required_review_without_retry':True,
                    'late_original_settled_without_duplicate':True,
                    'provider_effects_per_operation':1,
                },indent=2))
            finally:
                client.delete(stream)
    finally:
        with connect(admin) as c:
            c.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))


if __name__=='__main__':
    main()
