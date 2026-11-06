"""One HTTP request per call; redirects and proxy environment are disabled."""
from dataclasses import dataclass
from http.client import HTTPException
import hashlib
import json
import math
from urllib.error import HTTPError,URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler,ProxyHandler,Request,build_opener
from uuid import UUID

from retry_window_lab.domain import canonical_json

MAX_RESPONSE_BYTES=262144


def provider_url(value):
    if type(value) is not str:
        raise ValueError('Provider URL must be a string')
    parsed=urlsplit(value)
    try:
        port=parsed.port
    except ValueError:
        raise ValueError('Invalid provider port') from None
    if (parsed.scheme != 'http' or parsed.hostname not in {'127.0.0.1','localhost','::1'}
            or parsed.username is not None or parsed.password is not None
            or parsed.path not in ('','/') or parsed.query or parsed.fragment
            or port is None or not 1<=port<=65535):
        raise ValueError('Use a loopback HTTP origin with an explicit port')
    return value.rstrip('/')


def fingerprint(payload):
    return hashlib.sha256(canonical_json({'schema_version':1,'sku':payload['sku'],
                                          'quantity':payload['quantity']}).encode()).hexdigest()


@dataclass(frozen=True)
class Reply:
    status: int | None
    body: dict | None
    error: str | None = None


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):
        return None


class ProviderClient:
    def __init__(self,base_url,timeout=2):
        self.base_url=provider_url(base_url)
        if type(timeout) not in (int,float) or not math.isfinite(timeout) or not 0<timeout<=10:
            raise ValueError('HTTP timeout must be in (0,10] seconds')
        self.timeout=timeout
        self.opener=build_opener(ProxyHandler({}),NoRedirect())

    def request(self,path,body=None):
        data=None if body is None else canonical_json(body).encode('utf-8')
        request=Request(self.base_url+path,data=data,headers={'Content-Type':'application/json'},
                        method='GET' if data is None else 'POST')
        try:
            try:
                response=self.opener.open(request,timeout=self.timeout)
            except HTTPError as error:
                response=error
            with response:
                status=response.code
                raw=response.read(MAX_RESPONSE_BYTES+1)
            if len(raw)>MAX_RESPONSE_BYTES:
                return Reply(status,None,'RESPONSE_TOO_LARGE')
            def reject_constant(value):
                raise ValueError('Nonfinite JSON')
            parsed=json.loads(raw.decode('utf-8'),parse_constant=reject_constant)
            if type(parsed) is not dict:
                raise ValueError('Object response required')
            return Reply(status,parsed)
        except (ValueError,UnicodeError,RecursionError):
            return Reply(None,None,'INVALID_RESPONSE')
        except (OSError,URLError,TimeoutError,HTTPException):
            return Reply(None,None,'TRANSPORT_ERROR')

    def reserve(self,body):
        return self.request('/reservations',body)

    def lookup(self,experiment_id,operation_id):
        return self.request(f'/experiments/{experiment_id}/operations/{operation_id}')


def _integer(value,minimum=0):
    return type(value) is int and minimum<=value<=10**12+7200


def mutation_evidence(reply,context,body):
    """Return a bounded classification. Never infer failure from HTTP alone."""
    data=reply.body
    if reply.error or type(data) is not dict or reply.status not in (200,202,409):
        return 'UNKNOWN'
    required=('experiment_id','operation_id','request_id','fingerprint','sku','quantity',
              'profile','profile_version','window_seconds','provider_now','status','error_code','reused')
    if any(key not in data for key in required):
        return 'UNKNOWN'
    try:
        UUID(data['request_id'])
    except (ValueError,TypeError,AttributeError):
        return 'UNKNOWN'
    if (data['experiment_id']!=str(context['experiment_id'])
            or data['operation_id']!=body['operation_id']
            or data['fingerprint']!=fingerprint(body)
            or data['sku']!=body['sku'] or type(data['quantity']) is not int or data['quantity']!=body['quantity']
            or data['profile']!=context['profile']
            or type(data['profile_version']) is not int or data['profile_version']!=context['profile_version']
            or type(data['window_seconds']) is not int or data['window_seconds']!=context['window_seconds']
            or type(data['reused']) is not bool or not _integer(data['provider_now'])):
        return 'UNKNOWN'
    status=data['status']
    if status=='SUCCEEDED' and reply.status==200 and data['error_code'] is None:
        try:
            UUID(data['reservation_id'])
        except (ValueError,TypeError,KeyError,AttributeError):
            return 'UNKNOWN'
        accepted,expires=data.get('accepted_at'),data.get('protection_expires_at')
        if (not _integer(accepted) or not _integer(expires) or accepted>data['provider_now']
                or expires-accepted!=context['window_seconds']):
            return 'UNKNOWN'
        return 'EFFECT_CONFIRMED'
    if (status=='REJECTED' and reply.status==200 and data.get('reservation_id') is None
            and data.get('accepted_at') is None and data.get('protection_expires_at') is None
            and data['reused'] is False):
        if data.get('error_code')=='INSUFFICIENT_STOCK':
            return 'PERMANENT_REJECTION'
        if data.get('error_code')=='SETTLED_NO_EFFECT':
            return 'SETTLED_NO_EFFECT'
    if status=='CONFLICT' and reply.status==409 and data.get('error_code')=='KEY_CONFLICT':
        return 'PROVIDER_CONFLICT'
    return 'UNKNOWN'


def lookup_evidence(reply,context,body):
    data=reply.body
    if reply.error or reply.status!=200 or type(data) is not dict:
        return 'LOOKUP_INCONCLUSIVE'
    if (data.get('experiment_id')!=str(context['experiment_id'])
            or data.get('operation_id')!=body['operation_id']
            or data.get('profile')!=context['profile']
            or type(data.get('profile_version')) is not int or data['profile_version']!=context['profile_version']
            or not _integer(data.get('provider_now')) or type(data.get('reservations')) is not list):
        return 'INVALID_EVIDENCE'
    rows=data['reservations']
    if not rows:
        return 'LOOKUP_INCONCLUSIVE'  # NOT_FOUND and receipt lists never authorize a mutation.
    if len(rows)!=1:
        return 'DUPLICATE_EFFECT'
    row=rows[0]
    if type(row) is not dict:
        return 'INVALID_EVIDENCE'
    try:
        UUID(row['reservation_id'])
    except (KeyError,ValueError,TypeError,AttributeError):
        return 'INVALID_EVIDENCE'
    if (data.get('status')!='FOUND' or row.get('operation_id')!=body['operation_id']
            or row.get('fingerprint')!=fingerprint(body) or row.get('sku')!=body['sku']
            or type(row.get('quantity')) is not int or row['quantity']!=body['quantity']
            or not _integer(row.get('accepted_at')) or not _integer(row.get('visible_at'))
            or not row['accepted_at']<=row['visible_at']<=data['provider_now']):
        return 'INVALID_EVIDENCE'
    if context['profile']=='authoritative':
        requests=data.get('requests')
        if type(requests) is not list:
            return 'INVALID_EVIDENCE'
        if any(type(item) is not dict or item.get('status')=='PROCESSING' for item in requests):
            return 'LOOKUP_INCONCLUSIVE'
    return 'EFFECT_CONFIRMED'
