from dataclasses import replace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from retry_window_lab.provider import ExperimentMissing, LookupUnavailable, ReservationRequest
from retry_window_lab.provider_api import create_provider_app


def request():
    return ReservationRequest(uuid4(),uuid4(),'key','SKU-BOOK-001',2)


@pytest.mark.parametrize('field,value', [
    ('experiment_id','not-uuid'), ('operation_id',None), ('idempotency_key',''),
    ('idempotency_key','  '), ('idempotency_key','x'*129), ('idempotency_key',5),
    ('idempotency_key','\ud800'), ('idempotency_key','nul\x00key'), ('sku',[]), ('sku','UNKNOWN'), ('quantity',True),
    ('quantity',0), ('quantity',-1), ('quantity',2.5), ('quantity',2**31),
])
def test_invalid_reservation_definition(field,value):
    with pytest.raises((ValueError,UnicodeEncodeError)):
        replace(request(),**{field:value})


def test_fingerprint_is_business_data_not_dispatch_identity():
    original=request()
    assert replace(original,experiment_id=uuid4(),operation_id=uuid4(),idempotency_key='other').fingerprint == original.fingerprint
    assert replace(original,quantity=3).fingerprint != original.fingerprint
    assert replace(original,sku='SKU-PEN-001').fingerprint != original.fingerprint


class StubProvider:
    def reserve(self, request):
        return {'status':'SUCCEEDED','reservation_id':str(uuid4()),'response_lost':False}

    def lookup(self,*args):
        return {'status':'NOT_FOUND','reservations':[],'absence_authorizes_retry':False}


def body():
    return {'experiment_id':str(uuid4()),'operation_id':str(uuid4()),
            'idempotency_key':'key','sku':'SKU-BOOK-001','quantity':2}


def test_api_returns_success_and_no_internal_loss_flag():
    with TestClient(create_provider_app(StubProvider())) as client:
        response=client.post('/reservations',json=body())
        assert response.status_code == 200
        assert response.json()['status'] == 'SUCCEEDED'
        assert 'response_lost' not in response.json()


@pytest.mark.parametrize('status,http', [('PROCESSING',202),('CONFLICT',409),('REJECTED',200)])
def test_api_business_statuses(status,http):
    store=StubProvider()
    store.reserve=lambda _: {'status':status,'response_lost':False}
    with TestClient(create_provider_app(store)) as client:
        assert client.post('/reservations',json=body()).status_code == http


def test_lost_response_does_not_return_effect_evidence():
    store=StubProvider()
    store.reserve=lambda _: {'status':'SUCCEEDED','reservation_id':str(uuid4()),'response_lost':True}
    with TestClient(create_provider_app(store)) as client:
        response=client.post('/reservations',json=body())
        assert response.status_code == 503
        assert response.json() == {'detail':'simulated_response_lost'}


@pytest.mark.parametrize('field,value', [
    ('quantity',True),('quantity','2'),('quantity',0),('quantity',1.5),
    ('experiment_id','bad'),('sku','bad'),('idempotency_key',' '),('extra','x'),
])
def test_api_invalid_payload(field,value):
    data=body()
    data[field]=value
    with TestClient(create_provider_app(StubProvider())) as client:
        assert client.post('/reservations',json=data).status_code == 422


@pytest.mark.parametrize('error,status,detail', [
    (ExperimentMissing(),404,'experiment_not_found'),
    (LookupUnavailable(),503,'lookup_unavailable'),
])
def test_api_lookup_errors(error,status,detail):
    store=StubProvider()
    def fail(*args):
        raise error
    store.lookup=fail
    with TestClient(create_provider_app(store)) as client:
        response=client.get(f'/experiments/{uuid4()}/operations/{uuid4()}')
        assert response.status_code == status
        assert response.json() == {'detail':detail}


def test_health_and_control_routes():
    with TestClient(create_provider_app()) as client:
        assert client.get('/health').json() == {'status':'ok'}
        assert client.post('/advance',json={}).status_code == 404
        assert client.post('/inject',json={}).status_code == 404


def test_runtime_connections_use_separate_credentials(monkeypatch):
    import retry_window_lab.runtime_config as module
    from psycopg.conninfo import conninfo_to_dict
    monkeypatch.setattr(module,'postgres_conninfo',lambda: 'host=127.0.0.1 user=bootstrap password=bootstrap dbname=test')
    for key,value in {'RWL_APP_USER':'app','RWL_APP_PASSWORD':'app-secret',
                      'RWL_PROVIDER_USER':'provider','RWL_PROVIDER_PASSWORD':'provider-secret'}.items():
        monkeypatch.setenv(key,value)
    assert conninfo_to_dict(module.application_conninfo())['user'] == 'app'
    assert conninfo_to_dict(module.provider_conninfo())['user'] == 'provider'
    monkeypatch.delenv('RWL_APP_PASSWORD')
    with pytest.raises(KeyError):
        module.application_conninfo()
