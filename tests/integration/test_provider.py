from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.conninfo import make_conninfo

from retry_window_lab.config import postgres_conninfo
from retry_window_lab.db import connect,table
from retry_window_lab.domain import TaskDefinition
from retry_window_lab.migrate import migrate as app_migrate
from retry_window_lab.provider_migrate import MIGRATIONS,migrate
from retry_window_lab.provider import ExperimentMissing,LookupUnavailable,ProviderRepository,ReservationRequest
from retry_window_lab.provider_api import create_provider_app
from retry_window_lab.provision import configure_roles
from retry_window_lab.storage import TaskRepository
from retry_window_lab.ownership import Notification,OwnershipRepository
from retry_window_lab.worker import execute_claim

pytestmark=pytest.mark.integration


@pytest.fixture
def provider():
    conninfo=postgres_conninfo()
    schema=f'rwl_test_{uuid4().hex}'
    try:
        migrate(conninfo,schema)
        yield ProviderRepository(conninfo,schema)
    finally:
        with connect(conninfo) as c:
            c.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))


def request(experiment_id,quantity=2):
    return ReservationRequest(experiment_id,uuid4(),'stable-key','SKU-BOOK-001',quantity)


def totals(provider,experiment_id):
    with connect(provider.conninfo) as c:
        remaining=c.execute(sql.SQL("SELECT remaining FROM {} WHERE experiment_id=%s AND sku='SKU-BOOK-001'")
                            .format(table(provider.schema,'inventory')),(experiment_id,)).fetchone()['remaining']
        count=c.execute(sql.SQL('SELECT count(*) AS n FROM {} WHERE experiment_id=%s')
                        .format(table(provider.schema,'reservations')),(experiment_id,)).fetchone()['n']
    return remaining,count


def test_provider_migration_repeatable_and_ledger(provider):
    assert migrate(provider.conninfo,provider.schema) == []
    with connect(provider.conninfo) as c:
        rows=c.execute(sql.SQL('SELECT version FROM {} ORDER BY version')
                       .format(table(provider.schema,'schema_migrations'))).fetchall()
    assert [r['version'] for r in rows] == [version for version,_ in MIGRATIONS]


def test_modified_provider_migration_rejected(provider):
    with connect(provider.conninfo) as c:
        c.execute(sql.SQL("UPDATE {} SET checksum='changed' WHERE version=1")
                  .format(table(provider.schema,'schema_migrations')))
    with pytest.raises(RuntimeError,match='modified'):
        migrate(provider.conninfo,provider.schema)


def test_initial_stock_and_experiment_isolation(provider):
    first=provider.create_experiment()
    second=provider.create_experiment()
    provider.reserve(request(first,10))
    assert totals(provider,first) == (90,1)
    assert totals(provider,second) == (100,0)
    with connect(provider.conninfo) as c:
        rows=c.execute(sql.SQL('SELECT remaining FROM {} WHERE experiment_id=%s')
                       .format(table(provider.schema,'inventory')),(second,)).fetchall()
    assert [r['remaining'] for r in rows] == [100,100]


def test_same_protected_key_reuses_reservation_without_extending_window(provider):
    experiment=provider.create_experiment()
    mutation=request(experiment)
    first=provider.reserve(mutation)
    provider.advance(experiment,59)
    second=provider.reserve(mutation)
    assert first['reservation_id'] == second['reservation_id']
    assert first['accepted_at'] == second['accepted_at'] == 0
    assert second['protection_expires_at'] == 60
    assert second['reused'] is True
    assert first['request_id'] != second['request_id']
    assert totals(provider,experiment) == (98,1)


def test_conflicting_payload_within_protection_rejected(provider):
    experiment=provider.create_experiment()
    mutation=request(experiment)
    provider.reserve(mutation)
    result=provider.reserve(replace(mutation,quantity=3))
    assert result['status'] == 'CONFLICT' and result['error_code'] == 'KEY_CONFLICT'
    assert totals(provider,experiment) == (98,1)


def test_exact_expiry_boundary_can_create_duplicate_effect(provider):
    experiment=provider.create_experiment()
    mutation=request(experiment)
    first=provider.reserve(mutation)
    provider.advance(experiment,60)
    second=provider.reserve(mutation)
    assert first['reservation_id'] != second['reservation_id']
    assert not second['reused'] and second['accepted_at'] == 60
    assert totals(provider,experiment) == (96,2)
    assert len(provider.lookup(experiment,mutation.operation_id)['reservations']) == 2


def test_changed_payload_after_expiry_is_new_unprotected_effect(provider):
    experiment=provider.create_experiment()
    mutation=request(experiment)
    provider.reserve(mutation)
    provider.advance(experiment,60)
    assert provider.reserve(replace(mutation,quantity=3))['status'] == 'SUCCEEDED'
    assert totals(provider,experiment) == (95,2)


def test_same_key_is_scoped_by_operation_and_experiment(provider):
    first,second=provider.create_experiment(),provider.create_experiment()
    mutation=request(first)
    provider.reserve(mutation)
    provider.reserve(replace(mutation,operation_id=uuid4()))
    provider.reserve(replace(mutation,experiment_id=second))
    assert totals(provider,first) == (96,2)
    assert totals(provider,second) == (98,1)


def test_different_key_can_create_second_effect_for_same_operation(provider):
    experiment=provider.create_experiment()
    mutation=request(experiment)
    provider.reserve(mutation)
    provider.reserve(replace(mutation,idempotency_key='other'))
    assert totals(provider,experiment) == (96,2)


def test_concurrent_same_key_produces_one_reservation(provider):
    experiment=provider.create_experiment()
    mutation=request(experiment)
    barrier=Barrier(8)
    def reserve(_):
        barrier.wait(timeout=10)
        return provider.reserve(mutation)
    with ThreadPoolExecutor(max_workers=8) as pool:
        responses=list(pool.map(reserve,range(8)))
    assert len({r['reservation_id'] for r in responses}) == 1
    assert sum(r['reused'] for r in responses) == 7
    assert totals(provider,experiment) == (98,1)


def test_concurrent_reservations_cannot_oversell(provider):
    experiment=provider.create_experiment()
    barrier=Barrier(2)
    def reserve(_):
        barrier.wait(timeout=10)
        return provider.reserve(request(experiment,60))
    with ThreadPoolExecutor(max_workers=2) as pool:
        statuses=[r['status'] for r in pool.map(reserve,range(2))]
    assert sorted(statuses) == ['REJECTED','SUCCEEDED']
    assert totals(provider,experiment) == (40,1)


def test_insufficient_stock_is_permanent_no_new_effect(provider):
    experiment=provider.create_experiment()
    result=provider.reserve(request(experiment,101))
    assert result['status'] == 'REJECTED' and result['error_code'] == 'INSUFFICIENT_STOCK'
    assert totals(provider,experiment) == (100,0)


def test_delayed_lookup_visibility_boundary(provider):
    experiment=provider.create_experiment('delayed',visibility_delay=10)
    mutation=request(experiment)
    provider.reserve(mutation)
    assert provider.lookup(experiment,mutation.operation_id)['status'] == 'NOT_FOUND'
    provider.advance(experiment,9)
    assert provider.lookup(experiment,mutation.operation_id)['status'] == 'NOT_FOUND'
    provider.advance(experiment,1)
    result=provider.lookup(experiment,mutation.operation_id)
    assert result['status'] == 'FOUND' and len(result['reservations']) == 1
    assert result['requests'] == []


def test_lookup_unavailable_does_not_prevent_mutation(provider):
    experiment=provider.create_experiment('unavailable')
    mutation=request(experiment)
    assert provider.reserve(mutation)['status'] == 'SUCCEEDED'
    with pytest.raises(LookupUnavailable):
        provider.lookup(experiment,mutation.operation_id)


def test_authoritative_not_found_never_authorizes_retry(provider):
    experiment=provider.create_experiment()
    result=provider.lookup(experiment,uuid4())
    assert result['status'] == 'NOT_FOUND' and result['requests'] == []
    assert result['absence_authorizes_retry'] is False


def test_post_commit_response_loss_then_lookup_confirms_effect(provider):
    experiment=provider.create_experiment()
    mutation=request(experiment)
    provider.inject(experiment,'RESPONSE_LOST')
    data={key:str(value) if key.endswith('_id') else value for key,value in mutation.__dict__.items()}
    with TestClient(create_provider_app(provider)) as client:
        response=client.post('/reservations',json=data)
        assert response.status_code == 503
        assert response.json() == {'detail':'simulated_response_lost'}
    assert totals(provider,experiment) == (98,1)
    assert provider.lookup(experiment,mutation.operation_id)['status'] == 'FOUND'
    assert provider.reserve(mutation)['reused'] is True


def test_fault_is_consumed_once(provider):
    experiment=provider.create_experiment()
    provider.inject(experiment,'REJECT_NO_EFFECT')
    first=provider.reserve(request(experiment))
    assert first['status'] == 'REJECTED' and first['error_code'] == 'SETTLED_NO_EFFECT'
    assert provider.reserve(request(experiment))['status'] == 'SUCCEEDED'
    assert totals(provider,experiment) == (98,1)


def test_delayed_request_acceptance_starts_window_at_settlement(provider):
    experiment=provider.create_experiment()
    mutation=request(experiment)
    provider.inject(experiment,'DELAY_DELIVERY',20)
    response=provider.reserve(mutation)
    assert response['status'] == 'PROCESSING'
    assert response['accepted_at'] is None and response['protection_expires_at'] is None
    pending=provider.lookup(experiment,mutation.operation_id)
    assert pending['status'] == 'NOT_FOUND' and pending['requests'][0]['status'] == 'PROCESSING'
    assert totals(provider,experiment) == (100,0)
    assert provider.advance(experiment,19)['settled_requests'] == 0
    assert provider.advance(experiment,1)['settled_requests'] == 1
    found=provider.lookup(experiment,mutation.operation_id)
    assert found['reservations'][0]['accepted_at'] == 20
    assert provider.reserve(mutation)['protection_expires_at'] == 80


def test_old_delayed_request_can_commit_after_retry_protection_expires(provider):
    experiment=provider.create_experiment()
    mutation=request(experiment)
    provider.inject(experiment,'DELAY_DELIVERY',130)
    provider.reserve(mutation)
    provider.advance(experiment,60)
    negative=provider.lookup(experiment,mutation.operation_id)
    assert negative['status'] == 'NOT_FOUND' and not negative['absence_authorizes_retry']
    retry=provider.reserve(mutation)
    assert retry['protection_expires_at'] == 120
    provider.advance(experiment,70)
    assert totals(provider,experiment) == (96,2)


def test_old_request_inside_current_window_reuses_effect(provider):
    experiment=provider.create_experiment()
    mutation=request(experiment)
    provider.inject(experiment,'DELAY_DELIVERY',70)
    provider.reserve(mutation)
    provider.advance(experiment,60)
    provider.reserve(mutation)
    provider.advance(experiment,10)
    assert totals(provider,experiment) == (98,1)


def test_advancing_other_experiment_does_not_release_request(provider):
    first,second=provider.create_experiment(),provider.create_experiment()
    provider.inject(first,'DELAY_DELIVERY',20)
    provider.reserve(request(first))
    provider.advance(second,30)
    assert totals(provider,first) == (100,0)


def test_failed_reservation_insert_rolls_back_stock_request_and_fault(provider):
    experiment=provider.create_experiment()
    provider.inject(experiment,'RESPONSE_LOST')
    with connect(provider.conninfo) as c:
        c.execute(sql.SQL('''CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected reservation failure'; END; $$''')
            .format(table(provider.schema,'reject_reservation')))
        c.execute(sql.SQL('CREATE TRIGGER reject_reservation BEFORE INSERT ON {} FOR EACH ROW EXECUTE FUNCTION {}()')
                  .format(table(provider.schema,'reservations'),table(provider.schema,'reject_reservation')))
    with pytest.raises(psycopg.errors.RaiseException):
        provider.reserve(request(experiment))
    assert totals(provider,experiment) == (100,0)
    with connect(provider.conninfo) as c:
        assert c.execute(sql.SQL('SELECT count(*) AS n FROM {}').format(table(provider.schema,'requests'))).fetchone()['n'] == 0
        assert c.execute(sql.SQL('SELECT next_fault FROM {} WHERE experiment_id=%s')
                         .format(table(provider.schema,'experiments')),(experiment,)).fetchone()['next_fault'] == 'RESPONSE_LOST'


def test_unknown_experiment(provider):
    with pytest.raises(ExperimentMissing):
        provider.reserve(request(uuid4()))


@pytest.mark.parametrize('method,args', [
    ('create_experiment',('other',)), ('create_experiment',('authoritative',0)),
    ('advance',(uuid4(),-1)), ('advance',(uuid4(),True)),
    ('inject',(uuid4(),'OTHER')), ('inject',(uuid4(),'DELAY_DELIVERY',0)),
    ('inject',(uuid4(),'NONE',5)),
])
def test_invalid_operator_controls(provider,method,args):
    with pytest.raises(ValueError):
        getattr(provider,method)(*args)


@pytest.fixture
def isolated_roles():
    conninfo=postgres_conninfo()
    suffix=uuid4().hex
    app_schema,provider_schema=f'rwl_app_{suffix}',f'rwl_provider_{suffix}'
    app_role,provider_role=f'rwl_a_{suffix}',f'rwl_p_{suffix}'
    app_password,provider_password=uuid4().hex,uuid4().hex
    try:
        app_migrate(conninfo,app_schema)
        migrate(conninfo,provider_schema)
        configure_roles(conninfo,app_password,provider_password,app_role,provider_role,app_schema,provider_schema)
        yield (conninfo,app_schema,provider_schema,app_role,provider_role,
               make_conninfo(conninfo,user=app_role,password=app_password),
               make_conninfo(conninfo,user=provider_role,password=provider_password))
    finally:
        with connect(conninfo) as c:
            for schema in (app_schema,provider_schema):
                c.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
            for role in (app_role,provider_role):
                exists=c.execute('SELECT 1 FROM pg_roles WHERE rolname=%s',(role,)).fetchone()
                if exists:
                    c.execute(sql.SQL('DROP OWNED BY {}').format(sql.Identifier(role)))
                    c.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))


def test_restricted_application_role_can_submit_claim_and_finish(isolated_roles):
    admin,app_schema,provider_schema,app_role,provider_role,app,provider=isolated_roles
    repository=TaskRepository(app,app_schema)
    task=repository.submit(TaskDefinition('checksum','restricted',{'text':'abc'})).task
    with connect(app) as c:
        assert c.execute('SELECT current_user AS name').fetchone()['name'] == app_role
        event=c.execute(sql.SQL('SELECT event_id FROM {} WHERE task_id=%s').format(table(app_schema,'outbox')),
                        (task.task_id,)).fetchone()['event_id']
    ownership=OwnershipRepository(app,app_schema)
    _,claim=ownership.claim(Notification(event,task.task_id,1),'restricted-worker')
    assert execute_claim(ownership,claim,30) == 'SUCCEEDED'
    assert repository.get(task.task_id).state.value == 'SUCCEEDED'


def test_restricted_provider_role_can_mutate_and_lookup(isolated_roles):
    admin,app_schema,provider_schema,app_role,provider_role,app,provider=isolated_roles
    repository=ProviderRepository(provider,provider_schema)
    experiment=repository.create_experiment()
    mutation=request(experiment)
    assert repository.reserve(mutation)['status'] == 'SUCCEEDED'
    assert repository.lookup(experiment,mutation.operation_id)['status'] == 'FOUND'


@pytest.mark.parametrize('direction',['app_reads_provider','provider_reads_app','app_sets_provider','provider_sets_app','app_creates_schema_object','app_runs_migration'])
def test_runtime_roles_cannot_cross_boundary(isolated_roles,direction):
    admin,app_schema,provider_schema,app_role,provider_role,app,provider=isolated_roles
    cases={
        'app_reads_provider':(app,sql.SQL('SELECT * FROM {}').format(table(provider_schema,'reservations'))),
        'provider_reads_app':(provider,sql.SQL('SELECT * FROM {}').format(table(app_schema,'tasks'))),
        'app_sets_provider':(app,sql.SQL('SET ROLE {}').format(sql.Identifier(provider_role))),
        'provider_sets_app':(provider,sql.SQL('SET ROLE {}').format(sql.Identifier(app_role))),
        'app_creates_schema_object':(app,sql.SQL('CREATE TABLE {}(x INTEGER)').format(table(app_schema,'forbidden'))),
        'app_runs_migration':(app,sql.SQL('SELECT * FROM {}').format(table(app_schema,'schema_migrations'))),
    }
    conninfo,statement=cases[direction]
    with connect(conninfo) as c,pytest.raises(psycopg.errors.InsufficientPrivilege):
        c.execute(statement)


def test_role_provision_is_repeatable_and_nonprivileged(isolated_roles):
    admin,app_schema,provider_schema,app_role,provider_role,*_=isolated_roles
    configure_roles(admin,uuid4().hex,uuid4().hex,app_role,provider_role,app_schema,provider_schema)
    with connect(admin) as c:
        rows=c.execute('SELECT rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=ANY(%s)',
                       ([app_role,provider_role],)).fetchall()
    assert len(rows) == 2 and all(not any(row.values()) for row in rows)
