from typing import Annotated, Any, Literal
from uuid import UUID

import psycopg
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from retry_window_lab.config import postgres_conninfo
from retry_window_lab.domain import TaskDefinition
from retry_window_lab.local import SubmissionConflict
from retry_window_lab.storage import TaskRepository


class SubmissionBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    kind: Literal["checksum", "reserve_inventory"]
    submission_key: str = Field(min_length=1, max_length=128)
    payload: dict[str, Any]


def create_app(repository: TaskRepository | None = None) -> FastAPI:
    application = FastAPI(title="Retry Window Lab", version="0.1.0")

    def get_repository() -> TaskRepository:
        return repository if repository is not None else TaskRepository(postgres_conninfo())

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
            raise HTTPException(status_code=409, detail="submission_key_conflict") from None
        except psycopg.OperationalError:
            raise HTTPException(
                status_code=503, detail="database_unavailable", headers={"Retry-After": "1"},
            ) from None
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

    return application


app = create_app()
