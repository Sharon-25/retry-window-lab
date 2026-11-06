"""Operator demonstration. Mutation and lookup go through the provider HTTP service."""
import argparse
from dataclasses import asdict
import json
from urllib.error import HTTPError
from urllib.request import Request,urlopen
from uuid import UUID,uuid4

from retry_window_lab.config import postgres_conninfo
from retry_window_lab.provider import ProviderRepository,ReservationRequest


def http(base,path,body=None):
    data=None if body is None else json.dumps(body).encode()
    request=Request(base+path,data=data,headers={'Content-Type':'application/json'},
                    method='GET' if data is None else 'POST')
    try:
        with urlopen(request,timeout=10) as response:
            return response.status,json.load(response)
    except HTTPError as error:
        return error.code,json.load(error)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url',default='http://127.0.0.1:8001')
    args=parser.parse_args()
    base=args.base_url.rstrip('/')
    operator=ProviderRepository(postgres_conninfo())

    def mutation(experiment):
        return ReservationRequest(experiment,uuid4(),'stable-key','SKU-BOOK-001',2)

    def reserve(request):
        body={key:str(value) if isinstance(value,UUID) else value for key,value in asdict(request).items()}
        return http(base,'/reservations',body)

    def lookup(request):
        return http(base,f'/experiments/{request.experiment_id}/operations/{request.operation_id}')

    experiment=operator.create_experiment()
    request=mutation(experiment)
    status,first=reserve(request)
    assert status == 200 and first['status'] == 'SUCCEEDED'
    status,second=reserve(request)
    assert status == 200 and second['reused'] and second['reservation_id'] == first['reservation_id']
    operator.advance(experiment,60)
    status,third=reserve(request)
    assert status == 200 and third['reservation_id'] != first['reservation_id']
    assert len(lookup(request)[1]['reservations']) == 2

    lost=mutation(operator.create_experiment())
    operator.inject(lost.experiment_id,'RESPONSE_LOST')
    assert reserve(lost) == (503,{'detail':'simulated_response_lost'})
    assert lookup(lost)[1]['status'] == 'FOUND'

    delayed=mutation(operator.create_experiment('delayed',visibility_delay=10))
    assert reserve(delayed)[0] == 200
    assert lookup(delayed)[1]['status'] == 'NOT_FOUND'
    operator.advance(delayed.experiment_id,10)
    assert lookup(delayed)[1]['status'] == 'FOUND'

    unavailable=mutation(operator.create_experiment('unavailable'))
    assert reserve(unavailable)[0] == 200
    assert lookup(unavailable) == (503,{'detail':'lookup_unavailable'})

    late=mutation(operator.create_experiment())
    operator.inject(late.experiment_id,'DELAY_DELIVERY',130)
    assert reserve(late)[0] == 202
    operator.advance(late.experiment_id,60)
    negative=lookup(late)[1]
    assert negative['status'] == 'NOT_FOUND' and not negative['absence_authorizes_retry']
    assert reserve(late)[0] == 200  # Deliberately unsafe policy for this demonstration.
    operator.advance(late.experiment_id,70)
    assert len(lookup(late)[1]['reservations']) == 2

    print(json.dumps({
        'within_window_reused':True,
        'expiry_allowed_duplicate_effect':True,
        'lost_response_still_committed':True,
        'delayed_lookup_became_visible':True,
        'unavailable_lookup_reported':True,
        'late_original_created_duplicate':True,
        'note':'Duplicate effects are intentional experimental outcomes, not recovery guarantees.'
    },indent=2))


if __name__ == '__main__':
    main()
