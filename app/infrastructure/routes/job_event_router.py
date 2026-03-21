import json
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from persistence_kit import Repository
from persistence_kit.repository_factory import provide_repo

from app.application.dto.job_event_dto import JobEventCreate, JobEventOut
from app.application.use_cases.job_event_use_cases import (
    add_job_event as add_job_event_use_case,
    list_job_events as list_job_events_use_case,
)
from app.domain.entities.job_event import JobEvent
from app.domain.entities.job_execution import JobExecution


JobExecutionRepoDep = Annotated[Repository[JobExecution, UUID], Depends(provide_repo("job_execution"))]
JobEventRepoDep = Annotated[Repository[JobEvent, UUID], Depends(provide_repo("job_event"))]

router = APIRouter(prefix="/job-executions/{job_id}/events", tags=["Job Events"])


@router.post(
    "",
    response_model=JobEventOut,
    status_code=status.HTTP_201_CREATED,
    summary="Agregar evento",
)
async def add_event(
    job_id: UUID,
    payload: JobEventCreate,
    event_repo: JobEventRepoDep,
    job_repo: JobExecutionRepoDep,
):
    execution = await job_repo.get(job_id)
    if execution is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job execution not found")

    event = await add_job_event_use_case(event_repo, job_id, payload)
    return _serialize_job_event(event)


@router.get(
    "",
    response_model=list[JobEventOut],
    status_code=status.HTTP_200_OK,
    summary="Listar eventos",
)
async def list_events(
    job_id: UUID,
    event_repo: JobEventRepoDep,
):
    events = await list_job_events_use_case(event_repo, job_id)
    return [_serialize_job_event(event) for event in events]


def _serialize_job_event(event: JobEvent) -> dict[str, object]:
    return {
        "id": event.id,
        "job_execution_id": event.job_execution_id,
        "event_type": event.event_type,
        "message": event.message,
        "data": json.loads(event.data) if event.data else None,
        "level": event.level,
        "created_at": event.created_at,
    }
