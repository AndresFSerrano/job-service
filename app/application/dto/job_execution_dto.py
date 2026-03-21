from typing import Any
from uuid import UUID

from app.domain.entities.enums.job_execution_status import JobExecutionStatus
from pydantic import BaseModel


class JobExecutionCreate(BaseModel):
    job_key: str
    job_input: dict[str, Any] | None = None
    correlation_id: str | None = None
    requested_by_type: str = "user"
    requested_by_id: str | None = None
    requested_by_display: str | None = None
    priority: int = 0


class JobExecutionUpdate(BaseModel):
    status: JobExecutionStatus | None = None
    progress_current: int | None = None
    progress_total: int | None = None
    progress_label: str | None = None
    result: dict[str, Any] | None = None
    result_summary: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    worker_id: str | None = None
    inngest_run_id: str | None = None
    inngest_run_url: str | None = None
    started_at: str | None = None
    finished_at: str | None = None


class JobExecutionOut(BaseModel):
    id: UUID
    job_definition_id: UUID
    client_key: str
    job_key: str
    job_input: dict[str, Any] | None = None
    correlation_id: str | None = None
    status: JobExecutionStatus
    requested_by_type: str | None = None
    requested_by_id: str | None = None
    requested_by_display: str | None = None
    priority: int = 0
    progress_current: int | None = None
    progress_total: int | None = None
    progress_label: str | None = None
    result: dict[str, Any] | None = None
    result_summary: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    worker_id: str | None = None
    inngest_run_id: str | None = None
    inngest_run_url: str | None = None
    queued_at: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    created_at: str
    updated_at: str


class JobExecutionPageOut(BaseModel):
    items: list[JobExecutionOut]
    total: int
    page: int
    page_size: int
