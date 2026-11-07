from contextlib import asynccontextmanager
import os
from typing import Annotated, Any, Literal
from uuid import UUID

import psycopg
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel, ConfigDict, Field

from retry_window_lab.runtime_config import application_conninfo
from retry_window_lab.domain import TaskDefinition
from retry_window_lab.local import SubmissionConflict
from retry_window_lab.storage import TaskRepository
from retry_window_lab.capacity import QueueAtCapacity
from retry_window_lab.dashboard import DASHBOARD_HTML
from retry_window_lab.events import configure_event_output, emit_event
from retry_window_lab.monitoring import MonitoringRepository
from retry_window_lab.recovery import ContextConflict,RecoveryRepository


class SubmissionBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    kind: Literal["checksum", "reserve_inventory"]
    submission_key: str = Field(min_length=1, max_length=128)
    payload: dict[str, Any]


class ContextBody(BaseModel):
    model_config=ConfigDict(extra='forbid')
    experiment_id: UUID
    window_seconds: int = Field(default=60,strict=True,ge=1,le=3600)
    profile_version: int = Field(default=1,strict=True,ge=1,le=1)


def create_app(repository: TaskRepository | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app):
        if repository is None:
            configure_event_output()
        yield

    application = FastAPI(title="Retry Window Lab", version="0.1.0", lifespan=lifespan)

    def get_repository() -> TaskRepository:
        return repository if repository is not None else TaskRepository(application_conninfo())

    Repository = Annotated[TaskRepository, Depends(get_repository)]

    @application.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @application.post("/tasks", status_code=202)
    def submit_task(body: SubmissionBody, store: Repository) -> JSONResponse:
        try:
            definition = TaskDefinition(body.kind, body.submission_key, body.payload)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None
        try:
            result = store.submit(definition)
        except SubmissionConflict:
            emit_event("submission_rejected", status=409, reason="submission_key_conflict")
            raise HTTPException(status_code=409, detail="submission_key_conflict") from None
        except QueueAtCapacity:
            emit_event("submission_rejected", status=429, reason="queue_at_capacity")
            raise HTTPException(
                status_code=429, detail="queue_at_capacity", headers={"Retry-After": "1"},
            ) from None
        except (psycopg.OperationalError, psycopg.errors.LockNotAvailable,
                psycopg.errors.QueryCanceled):
            raise HTTPException(
                status_code=503, detail="database_unavailable", headers={"Retry-After": "1"},
            ) from None
        emit_event("task_submitted" if result.created else "task_replayed",
                   task_id=result.task.task_id, operation_id=result.task.operation_id,
                   kind=definition.kind.value)
        return JSONResponse(
            status_code=202 if result.created else 200,
            headers={"Location": f"/tasks/{result.task.task_id}"},
            content={"created": result.created, "task": result.task.as_dict()},
        )

    @application.get("/tasks/{task_id}")
    def get_task(task_id: UUID, store: Repository) -> dict:
        try:
            task = store.get(task_id)
        except psycopg.OperationalError:
            raise HTTPException(status_code=503, detail="database_unavailable") from None
        if task is None:
            raise HTTPException(status_code=404, detail="task_not_found")
        return task.as_dict()

    def recovery_store(store):
        return RecoveryRepository(store._conninfo,store._schema)

    @application.post('/tasks/{task_id}/provider-context')
    def bind_context(task_id: UUID,body: ContextBody,store: Repository):
        try:
            created=recovery_store(store).bind(task_id,body.experiment_id,
                os.environ.get('RWL_PROVIDER_URL','http://127.0.0.1:8001'),body.window_seconds,body.profile_version)
        except ContextConflict:
            raise HTTPException(409,'provider_context_conflict') from None
        except LookupError:
            raise HTTPException(404,'task_not_found') from None
        except ValueError:
            raise HTTPException(422,'invalid_provider_context') from None
        except psycopg.OperationalError:
            raise HTTPException(503,'database_unavailable') from None
        if created:
            emit_event('provider_context_bound', task_id=task_id, experiment_id=body.experiment_id)
        return {'created':created,'task_id':str(task_id)}

    @application.get('/tasks/{task_id}/recovery')
    def recovery_status(task_id: UUID,store: Repository):
        try:
            return recovery_store(store).audit(task_id)
        except LookupError:
            raise HTTPException(404,'task_not_found') from None
        except psycopg.OperationalError:
            raise HTTPException(503,'database_unavailable') from None

    @application.get("/", include_in_schema=False)
    def root():
        return RedirectResponse("/dashboard", status_code=307)

    @application.get("/dashboard", include_in_schema=False)
    def dashboard():
        return HTMLResponse(DASHBOARD_HTML, headers={"Cache-Control": "no-store"})

    @application.get("/monitoring/summary")
    def monitoring_summary(store: Repository) -> dict:
        try:
            return MonitoringRepository(store._conninfo, store._schema).summary()
        except (psycopg.OperationalError, psycopg.errors.QueryCanceled):
            raise HTTPException(503, "database_unavailable") from None

    @application.get("/monitoring/tasks/{task_id}")
    def monitoring_task(task_id: UUID, store: Repository) -> dict:
        try:
            return MonitoringRepository(store._conninfo, store._schema).task_history(task_id)
        except LookupError:
            raise HTTPException(404, "task_not_found") from None
        except (psycopg.OperationalError, psycopg.errors.QueryCanceled):
            raise HTTPException(503, "database_unavailable") from None

    return application


app = create_app()
