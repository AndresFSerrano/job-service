from typing import Any
from uuid import UUID

from pydantic import BaseModel


class JobDefinitionCreate(BaseModel):
    client_key: str
    job_key: str
    display_name: str
    version: int = 1
    description: str | None = None
    payload_schema: dict[str, Any] | None = None
    result_schema: dict[str, Any] | None = None
    visibility_scope: str | None = None
    retry_policy: dict[str, Any] | None = None
    execution_engine: str = "local"
    execution_ref: str | None = None
    active: bool = True
    allow_concurrent: bool = True


class JobDefinitionOut(JobDefinitionCreate):
    id: UUID
    created_at: str
