from typing import Any
from uuid import UUID

from pydantic import BaseModel

from app.application.dto.job_definition_dto import JobDefinitionCreate, JobDefinitionOut


class JobClientCreate(BaseModel):
    client_key: str
    display_name: str
    base_url: str
    description: str | None = None
    status: str = "active"
    callback_auth_mode: str | None = None
    healthcheck_url: str | None = None
    metadata: dict[str, Any] | str | None = None


class JobClientOut(JobClientCreate):
    id: UUID
    created_at: str


class ServiceRegistrationCreate(JobClientCreate):
    job_definitions: list[JobDefinitionCreate]


class ServiceRegistrationOut(BaseModel):
    client: JobClientOut
    job_definitions: list[JobDefinitionOut]
