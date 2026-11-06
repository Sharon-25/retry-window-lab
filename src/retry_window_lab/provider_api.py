from typing import Annotated
from uuid import UUID

import psycopg
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from retry_window_lab.runtime_config import provider_conninfo
from retry_window_lab.provider import (
    ExperimentMissing, LookupUnavailable, ProviderRepository, ReservationRequest,
)


class ReservationBody(BaseModel):
    model_config = ConfigDict(extra='forbid')
    experiment_id: UUID
    operation_id: UUID
    idempotency_key: str = Field(strict=True, min_length=1, max_length=128)
    sku: str = Field(strict=True)
    quantity: int = Field(strict=True, ge=1, le=2**31-1)


def create_provider_app(repository=None):
    app = FastAPI(title='Retry Window Lab — Provider Simulator', version='0.1.0')

    def get_repository():
        return repository if repository is not None else ProviderRepository(provider_conninfo())

    Store = Annotated[ProviderRepository, Depends(get_repository)]

    def invoke(function, *args):
        try:
            return function(*args)
        except ExperimentMissing:
            raise HTTPException(404, 'experiment_not_found') from None
        except LookupUnavailable:
            raise HTTPException(503, 'lookup_unavailable') from None
        except ValueError:
            raise HTTPException(422, 'invalid_provider_request') from None
        except psycopg.OperationalError:
            raise HTTPException(503, 'provider_database_unavailable') from None

    @app.get('/health')
    def health():
        return {'status': 'ok'}

    @app.post('/reservations')
    def reserve(body: ReservationBody, store: Store):
        try:
            request = ReservationRequest(**body.model_dump())
        except ValueError:
            raise HTTPException(422, 'invalid_provider_request') from None
        response = invoke(store.reserve, request)
        if response['response_lost']:
            # Simulate a lost post-commit response using a deterministic HTTP error.
            # No reservation ID or receipt is returned to the caller in this case.
            raise HTTPException(503, 'simulated_response_lost')
        response = {key:value for key,value in response.items() if key != 'response_lost'}
        status = 202 if response['status'] == 'PROCESSING' else 409 if response['status'] == 'CONFLICT' else 200
        return JSONResponse(status_code=status, content=response)

    @app.get('/experiments/{experiment_id}/operations/{operation_id}')
    def lookup(experiment_id: UUID, operation_id: UUID, store: Store):
        return invoke(store.lookup, experiment_id, operation_id)

    return app


app = create_provider_app()
