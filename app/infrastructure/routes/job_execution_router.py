import json
import asyncio
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from persistence_kit import Repository
from persistence_kit.repository_factory import provide_repo
from starlette.responses import StreamingResponse

from app.application.dto.job_event_dto import JobCheckpointCreate, JobCheckpointOut
from app.application.dto.job_execution_dto import (
    JobExecutionCreate,
    JobExecutionOut,
    JobExecutionPageOut,
    JobExecutionUpdate,
)
from app.application.use_cases.job_event_use_cases import (
    checkpoint_job_execution as checkpoint_job_execution_use_case,
)
from app.application.use_cases.job_execution_use_cases import (
    create_job_execution as create_job_execution_use_case,
    get_job_execution as get_job_execution_use_case,
    get_job_execution_by_id_or_worker_id as get_job_execution_by_ref_use_case,
    list_job_executions as list_job_executions_use_case,
    list_job_execution_requesters as list_job_execution_requesters_use_case,
    list_job_execution_statuses as list_job_execution_statuses_use_case,
    update_job_execution as update_job_execution_use_case,
)
from app.core.config import Settings, get_settings
from app.domain.entities.job_definition import JobDefinition
from app.domain.entities.job_event import JobEvent
from app.domain.entities.job_execution import JobExecution
from app.domain.security.authenticated_user import AuthenticatedUser
from app.infrastructure.security.authorization import (
    ensure_user_can_access_manager_type,
    get_current_user,
)
from app.realtime.job_execution_stream import serialize_job_execution, stream_events
from app.workers.inngest_dispatcher import get_dispatcher


JobExecutionRepoDep = Annotated[Repository[JobExecution, UUID], Depends(provide_repo("job_execution"))]
JobDefinitionRepoDep = Annotated[
    Repository[JobDefinition, UUID],
    Depends(provide_repo("job_definition")),
]
JobEventRepoDep = Annotated[Repository[JobEvent, UUID], Depends(provide_repo("job_event"))]

router = APIRouter(prefix="/job-executions", tags=["Job Executions"])


@router.post(
    "",
    response_model=JobExecutionOut,
    status_code=status.HTTP_201_CREATED,
    summary="Crear ejecución de job",
)
async def create_job_execution(
    payload: JobExecutionCreate,
    job_execution_repo: JobExecutionRepoDep,
    definition_repo: JobDefinitionRepoDep,
    current_user: AuthenticatedUser = Depends(get_current_user),
    settings: Settings = Depends(get_settings),
):
    try:
        job_input = dict(payload.job_input or {})
        manager_type = job_input.get("manager_type")
        ensure_user_can_access_manager_type(current_user, str(manager_type) if manager_type is not None else None)
        if settings.auth_enabled:
            payload = payload.model_copy(
                update={
                    "requested_by_type": "user",
                    "requested_by_id": current_user.username,
                    "requested_by_display": current_user.email or current_user.username,
                }
            )
        execution = await create_job_execution_use_case(job_execution_repo, definition_repo, payload)
        dispatcher = get_dispatcher()
        if dispatcher is not None:
            asyncio.create_task(dispatcher.dispatch_execution(execution.id))
        return _serialize_job_execution(execution)
    except ValueError as exc:
        msg = str(exc)
        if "ya tiene una ejecución activa" in msg or "not globally unique" in msg:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=msg)
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=msg)


@router.get(
    "/status-options",
    status_code=status.HTTP_200_OK,
    summary="Listar estados válidos de ejecución",
)
async def list_job_execution_statuses():
    return await list_job_execution_statuses_use_case()


@router.get(
    "/requester-options",
    status_code=status.HTTP_200_OK,
    summary="Listar usuarios con ejecuciones",
)
async def list_job_execution_requesters(
    job_execution_repo: JobExecutionRepoDep,
    client_key: str | None = Query(default=None),
    job_key: str | None = Query(default=None),
    current_user: AuthenticatedUser = Depends(get_current_user),
):
    if not current_user.is_admin_general:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No tiene permisos para consultar usuarios.")
    return await list_job_execution_requesters_use_case(
        job_execution_repo,
        client_key=client_key,
        job_key=job_key,
    )


@router.get(
    "",
    response_model=JobExecutionPageOut,
    status_code=status.HTTP_200_OK,
    summary="Listar ejecuciones",
)
async def list_job_executions(
    job_execution_repo: JobExecutionRepoDep,
    client_key: str | None = Query(default=None),
    job_key: str | None = Query(default=None),
    requested_by_id: str | None = Query(default=None),
    requested_by: str | None = Query(default=None),
    status: list[str] | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    current_user: AuthenticatedUser = Depends(get_current_user),
):
    effective_requested_by_id = requested_by_id if current_user.is_admin_general else current_user.username
    executions, total = await list_job_executions_use_case(
        job_execution_repo,
        client_key=client_key,
        job_key=job_key,
        requested_by_id=effective_requested_by_id,
        requested_by=requested_by,
        statuses=status,
        page=page,
        page_size=page_size,
    )
    return {
        "items": [_serialize_job_execution(execution) for execution in executions],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get(
    "/{job_ref}",
    response_model=JobExecutionOut,
    status_code=status.HTTP_200_OK,
    summary="Consultar ejecución por ID o worker_id",
)
async def get_job_execution(
    job_ref: str,
    job_execution_repo: JobExecutionRepoDep,
    current_user: AuthenticatedUser = Depends(get_current_user),
):
    execution = await get_job_execution_by_ref_use_case(job_execution_repo, job_ref)
    if execution is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job execution not found")
    if not current_user.is_admin_general and execution.requested_by_id != current_user.username:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No tiene permisos para consultar esta ejecución.")
    return _serialize_job_execution(execution)


@router.get(
    "/{job_ref}/stream",
    status_code=status.HTTP_200_OK,
    summary="Escuchar cambios de una ejecución vía SSE",
)
async def stream_job_execution(
    job_ref: str,
    request: Request,
    job_execution_repo: JobExecutionRepoDep,
    current_user: AuthenticatedUser = Depends(get_current_user),
):
    execution = await get_job_execution_by_ref_use_case(job_execution_repo, job_ref)
    if execution is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job execution not found")
    if not current_user.is_admin_general and execution.requested_by_id != current_user.username:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No tiene permisos para consultar esta ejecución.")

    async def _event_generator():
        async for chunk in stream_events(execution.id, initial_execution=execution):
            if await request.is_disconnected():
                break
            yield chunk

    return StreamingResponse(
        _event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.patch(
    "/{job_id}",
    response_model=JobExecutionOut,
    status_code=status.HTTP_200_OK,
    summary="Actualizar estado de ejecución",
)
async def update_job_execution(
    job_id: UUID,
    payload: JobExecutionUpdate,
    job_execution_repo: JobExecutionRepoDep,
):
    try:
        execution = await update_job_execution_use_case(job_execution_repo, job_id, payload)
        return _serialize_job_execution(execution)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))


@router.post(
    "/{job_id}/checkpoint",
    response_model=JobCheckpointOut,
    status_code=status.HTTP_200_OK,
    summary="Registrar eventos y progreso en lote",
)
async def checkpoint_job_execution(
    job_id: UUID,
    payload: JobCheckpointCreate,
    job_execution_repo: JobExecutionRepoDep,
    event_repo: JobEventRepoDep,
):
    execution = await get_job_execution_use_case(job_execution_repo, job_id)
    if execution is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job execution not found")
    events_created = await checkpoint_job_execution_use_case(
        event_repo, job_execution_repo, job_id, payload
    )
    return {"events_created": events_created}


def _serialize_job_execution(execution: JobExecution) -> dict[str, object]:
    serialized = serialize_job_execution(execution)
    serialized["id"] = execution.id
    serialized["job_definition_id"] = execution.job_definition_id
    serialized["status"] = execution.status
    serialized["result"] = json.loads(execution.result) if execution.result else None
    return serialized
