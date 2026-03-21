from dataclasses import dataclass, field
from uuid import UUID, uuid4

from app.core.time import now_bogota_iso
from app.domain.entities.enums.job_execution_status import JobExecutionStatus


@dataclass(slots=True)
class JobExecution:
    job_definition_id: UUID
    client_key: str
    job_key: str
    payload: str | None = None
    correlation_id: str | None = None
    status: JobExecutionStatus = JobExecutionStatus.REGISTERED
    requested_by_type: str | None = None
    requested_by_id: str | None = None
    requested_by_display: str | None = None
    priority: int = 0
    progress_current: int | None = None
    progress_total: int | None = None
    progress_label: str | None = None
    result_summary: str | None = None
    result: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    worker_id: str | None = None
    inngest_run_id: str | None = None
    inngest_run_url: str | None = None
    queued_at: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    created_at: str = field(default_factory=now_bogota_iso)
    updated_at: str = field(default_factory=now_bogota_iso)
    id: UUID = field(default_factory=uuid4)
