from copy import deepcopy
import io
import json
import random
from urllib.error import HTTPError,URLError
from uuid import uuid4

import pytest

from retry_window_lab.provider_client import (
    MAX_RESPONSE_BYTES,ProviderClient,Reply,fingerprint,lookup_evidence,mutation_evidence,provider_url,
)
from retry_window_lab.recovery import RecoveryRepository


def sample():
    context={'experiment_id':uuid4(),'profile':'authoritative','profile_version':1,'window_seconds':60}
    body={'experiment_id':str(context['experiment_id']),'operation_id':str(uuid4()),
          'idempotency_key':'stable','sku':'SKU-BOOK-001','quantity':2}
    result={'experiment_id':body['experiment_id'],'operation_id':body['operation_id'],
            'request_id':str(uuid4()),'reservation_id':str(uuid4()),'fingerprint':fingerprint(body),
            'sku':body['sku'],'quantity':2,'profile':'authoritative','profile_version':1,
            'window_seconds':60,'provider_now':5,'accepted_at':5,'protection_expires_at':65,
            'status':'SUCCEEDED','error_code':None,'reused':False}
    return context,body,result


@pytest.mark.parametrize('value',[
    'https://127.0.0.1:8001','http://example.com:8001','http://127.0.0.1',
    'http://user:password@127.0.0.1:8001','http://127.0.0.1:8001/path',
    'http://127.0.0.1:8001?query=1','http://127.0.0.1:8001#fragment',
    'http://127.0.0.1:99999','http://127.0.0.1:invalid',None,
])
def test_bad_provider_origin(value):
    with pytest.raises(ValueError):
        provider_url(value)


def test_loopback_origin_normalized():
    assert provider_url('http://127.0.0.1:8001/')=='http://127.0.0.1:8001'


@pytest.mark.parametrize('field,value',[
    ('experiment_id',str(uuid4())),('operation_id',str(uuid4())),('request_id','bad'),
    ('fingerprint','bad'),('sku','SKU-PEN-001'),('quantity',True),('quantity',3),
    ('profile','delayed'),('profile_version',True),('profile_version',2),
    ('window_seconds',61),('window_seconds',True),('provider_now',False),
    ('accepted_at',6),('accepted_at',-1),('protection_expires_at',66),('reservation_id','bad'),
])
def test_mismatched_mutation_evidence_is_unknown(field,value):
    context,body,result=sample()
    result[field]=value
    assert mutation_evidence(Reply(200,result),context,body)=='UNKNOWN'


def test_confirmed_mutation():
    context,body,result=sample()
    assert mutation_evidence(Reply(200,result),context,body)=='EFFECT_CONFIRMED'


@pytest.mark.parametrize('error,reason',[
    ('SETTLED_NO_EFFECT','SETTLED_NO_EFFECT'),('INSUFFICIENT_STOCK','PERMANENT_REJECTION'),
    ('other','UNKNOWN'),
])
def test_no_effect_requires_matching_explicit_contract(error,reason):
    context,body,result=sample()
    result.update(status='REJECTED',reservation_id=None,accepted_at=None,protection_expires_at=None,error_code=error)
    assert mutation_evidence(Reply(200,result),context,body)==reason
    result['operation_id']=str(uuid4())
    assert mutation_evidence(Reply(200,result),context,body)=='UNKNOWN'


def lookup_sample():
    context,body,result=sample()
    row={key:result[key] for key in ('reservation_id','operation_id','fingerprint','sku','quantity','accepted_at')}
    row['visible_at']=5
    lookup={'experiment_id':body['experiment_id'],'operation_id':body['operation_id'],
            'provider_now':5,'profile':'authoritative','profile_version':1,'status':'FOUND',
            'reservations':[row],'requests':[],'absence_authorizes_retry':False}
    return context,body,lookup


def test_matching_lookup_can_confirm_effect():
    context,body,result=lookup_sample()
    assert lookup_evidence(Reply(200,result),context,body)=='EFFECT_CONFIRMED'


@pytest.mark.parametrize('profile',['authoritative','delayed','unavailable'])
def test_negative_observation_never_grants_retry(profile):
    context,body,result=lookup_sample()
    context['profile']=result['profile']=profile
    result.update(status='NOT_FOUND',reservations=[],absence_authorizes_retry=True)
    assert lookup_evidence(Reply(200,result),context,body)=='LOOKUP_INCONCLUSIVE'


def test_duplicate_effects_require_review():
    context,body,result=lookup_sample()
    result['reservations'].append(deepcopy(result['reservations'][0]))
    assert lookup_evidence(Reply(200,result),context,body)=='DUPLICATE_EFFECT'


@pytest.mark.parametrize('field,value',[
    ('quantity',True),('fingerprint','changed'),('operation_id',str(uuid4())),
    ('visible_at',6),('accepted_at',-1),('reservation_id','bad'),
])
def test_lookup_matching_is_strict(field,value):
    context,body,result=lookup_sample()
    result['reservations'][0][field]=value
    assert lookup_evidence(Reply(200,result),context,body)=='INVALID_EVIDENCE'


def test_transport_unknown_and_unavailable_lookup():
    context,body,_=sample()
    assert mutation_evidence(Reply(None,None,'TRANSPORT_ERROR'),context,body)=='UNKNOWN'
    assert lookup_evidence(Reply(503,{'detail':'lookup_unavailable'}),context,body)=='LOOKUP_INCONCLUSIVE'


class Response(io.BytesIO):
    code=200


@pytest.mark.parametrize('raw,expected',[
    (b'not json','INVALID_RESPONSE'),(b'[]','INVALID_RESPONSE'),
    (b'{"value":NaN}','INVALID_RESPONSE'),(b'\xff','INVALID_RESPONSE'),
    (b'x'*(MAX_RESPONSE_BYTES+1),'RESPONSE_TOO_LARGE'),
])
def test_http_response_bounds(raw,expected):
    client=ProviderClient('http://127.0.0.1:8001')
    class Opener:
        def open(self,*args,**kwargs):
            return Response(raw)
    client.opener=Opener()
    assert client.lookup(uuid4(),uuid4()).error==expected


def test_http_transport_is_not_retried():
    client=ProviderClient('http://127.0.0.1:8001')
    class Opener:
        calls=0
        def open(self,*args,**kwargs):
            self.calls+=1
            raise URLError('sensitive transport message')
    opener=Opener()
    client.opener=opener
    result=client.reserve({'payload':'value'})
    assert result==Reply(None,None,'TRANSPORT_ERROR') and opener.calls==1


def test_retry_jitter_is_bounded_and_seedable():
    first=RecoveryRepository('',rng=random.Random(7))
    second=RecoveryRepository('',rng=random.Random(7))
    a=[first.delay(number) for number in range(1,5)]
    assert a==[second.delay(number) for number in range(1,5)]
    assert all(0<=delay<=cap for delay,cap in zip(a,[1,2,4,8]))


def test_confirmed_effect_with_a_competing_pending_request_stays_inconclusive():
    context,body,result=lookup_sample()
    result['requests']=[{'request_id':str(uuid4()),'status':'PROCESSING'}]
    assert lookup_evidence(Reply(200,result),context,body)=='LOOKUP_INCONCLUSIVE'


def test_contradictory_no_effect_reply_is_not_retry_evidence():
    context,body,result=sample()
    result.update(status='REJECTED',reservation_id=None,error_code='SETTLED_NO_EFFECT')
    assert mutation_evidence(Reply(200,result),context,body)=='UNKNOWN'
