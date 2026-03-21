from typing import Any
from uuid import UUID

from pydantic import BaseModel


class JobEventCreate(BaseModel):
    event_type: str
    message: str
    data: dict[str, Any] | None = None
    level: str = "info"


class JobEventOut(JobEventCreate):
    id: UUID
    job_execution_id: UUID
    created_at: str


class JobCheckpointCreate(BaseModel):
    events: list[JobEventCreate] = []
    progress_current: int | None = None
    progress_total: int | None = None
    progress_label: str | None = None


class JobCheckpointOut(BaseModel):
    events_created: int
