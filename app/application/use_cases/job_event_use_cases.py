import json
from typing import Sequence
from uuid import UUID

from persistence_kit import Repository

from app.core.time import now_bogota_iso
from app.application.dto.job_event_dto import JobCheckpointCreate, JobEventCreate
from app.domain.entities.job_event import JobEvent
from app.domain.entities.job_execution import JobExecution
from app.realtime.job_execution_stream import publish_execution_update, publish_job_event


async def add_job_event(
    event_repo: Repository[JobEvent, UUID],
    job_id: UUID,
    payload: JobEventCreate,
) -> JobEvent:
    entity = JobEvent(
        job_execution_id=job_id,
        event_type=payload.event_type,
        message=payload.message,
        data=json.dumps(payload.data, sort_keys=True) if payload.data is not None else None,
        level=payload.level,
    )
    await event_repo.add(entity)
    await publish_job_event(entity)
    return entity


async def checkpoint_job_execution(
    event_repo: Repository[JobEvent, UUID],
    execution_repo: Repository[JobExecution, UUID],
    job_id: UUID,
    payload: JobCheckpointCreate,
) -> int:
    for event_create in payload.events:
        entity = JobEvent(
            job_execution_id=job_id,
            event_type=event_create.event_type,
            message=event_create.message,
            data=json.dumps(event_create.data, sort_keys=True) if event_create.data is not None else None,
            level=event_create.level,
        )
        await event_repo.add(entity)
        await publish_job_event(entity)

    has_progress = any(
        v is not None
        for v in (payload.progress_current, payload.progress_total, payload.progress_label)
    )
    if has_progress:
        execution = await execution_repo.get(job_id)
        if execution is not None:
            if payload.progress_current is not None:
                execution.progress_current = payload.progress_current
            if payload.progress_total is not None:
                execution.progress_total = payload.progress_total
            if payload.progress_label is not None:
                execution.progress_label = payload.progress_label
            execution.updated_at = now_bogota_iso()
            await execution_repo.update(execution)
            await publish_execution_update(execution)

    return len(payload.events)


async def list_job_events(
    event_repo: Repository[JobEvent, UUID],
    job_id: UUID,
) -> Sequence[JobEvent]:
    return await event_repo.list_by_fields({"job_execution_id": job_id}, limit=200)
