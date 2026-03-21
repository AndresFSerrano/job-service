import json
from uuid import UUID

from persistence_kit import Repository

from app.core.config import get_settings
from app.core.time import now_bogota_iso
from app.application.dto.job_execution_dto import (
    JobExecutionCreate,
    JobExecutionUpdate,
)
from app.domain.entities.enums.job_execution_status import JobExecutionStatus
from app.domain.entities.job_definition import JobDefinition
from app.domain.entities.job_execution import JobExecution
from app.realtime.job_execution_stream import publish_execution_update

_ACTIVE_STATUSES = {
    JobExecutionStatus.QUEUED,
    JobExecutionStatus.RUNNING,
    JobExecutionStatus.WAITING_CALLBACK,
}

_TERMINAL_STATUSES = {
    JobExecutionStatus.COMPLETED,
    JobExecutionStatus.FAILED,
    JobExecutionStatus.CANCELLED,
}

_STATUS_LABELS = {
    JobExecutionStatus.REGISTERED: "Registrado",
    JobExecutionStatus.QUEUED: "En cola",
    JobExecutionStatus.RUNNING: "En proceso",
    JobExecutionStatus.WAITING_CALLBACK: "Esperando respuesta",
    JobExecutionStatus.COMPLETED: "Completado",
    JobExecutionStatus.FAILED: "Fallido",
    JobExecutionStatus.CANCELLED: "Cancelado",
}


def _build_inngest_run_url(inngest_run_id: str | None) -> str | None:
    if not inngest_run_id:
        return None
    settings = get_settings()
    base_url = settings.inngest_public_url or settings.inngest_base_url
    if not base_url:
        return None
    return f"{base_url.rstrip('/')}/run?runID={inngest_run_id}"


async def list_job_execution_statuses() -> list[dict[str, str]]:
    return [
        {"value": status.value, "label": _STATUS_LABELS.get(status, status.value)}
        for status in JobExecutionStatus
    ]


async def list_job_execution_requesters(
    execution_repo: Repository[JobExecution, UUID],
    *,
    job_key: str | None = None,
    client_key: str | None = None,
) -> list[dict[str, str]]:
    filters: dict[str, str] = {}
    if client_key is not None:
        filters["client_key"] = client_key
    if job_key is not None:
        filters["job_key"] = job_key

    executions = await execution_repo.list_by_fields(filters, limit=1000) if filters else await execution_repo.list(limit=1000)
    options_by_value: dict[str, dict[str, str]] = {}

    for execution in executions:
        value = execution.requested_by_id
        label = execution.requested_by_display

        if execution.payload:
            try:
                payload = json.loads(execution.payload)
            except json.JSONDecodeError:
                payload = None
            if isinstance(payload, dict):
                payload_user_id = payload.get("user_id")
                if value is None and payload_user_id is not None:
                    value = str(payload_user_id)

        if not value:
            continue

        normalized_value = value.strip()
        if not normalized_value:
            continue

        display_label = label.strip() if isinstance(label, str) and label.strip() else normalized_value
        options_by_value[normalized_value] = {
            "value": normalized_value,
            "label": display_label,
        }

    return sorted(options_by_value.values(), key=lambda option: option["label"].lower())


async def create_job_execution(
    execution_repo: Repository[JobExecution, UUID],
    definition_repo: Repository[JobDefinition, UUID],
    payload: JobExecutionCreate,
) -> JobExecution:
    definitions = await definition_repo.list_by_fields({"job_key": payload.job_key})
    if not definitions:
        raise ValueError(f"job definition not found: {payload.job_key}")
    if len(definitions) > 1:
        raise ValueError(f"job_key '{payload.job_key}' is not globally unique")
    definition = definitions[0]

    # Idempotencia: si ya existe una ejecución con el mismo correlation_id, retornarla
    if payload.correlation_id is not None:
        existing = await execution_repo.list_by_fields({"correlation_id": payload.correlation_id}, limit=1)
        if existing:
            return existing[0]

    # Concurrencia: si la definición no permite ejecuciones simultáneas, bloquear
    if not definition.allow_concurrent:
        active = await execution_repo.list_by_fields(
            {"client_key": definition.client_key, "job_key": payload.job_key},
            limit=100,
        )
        if any(e.status in _ACTIVE_STATUSES for e in active):
            raise ValueError(
                f"job '{payload.job_key}' ya tiene una ejecución activa y no permite concurrencia"
            )

    execution = JobExecution(
        job_definition_id=definition.id,
        client_key=definition.client_key,
        job_key=definition.job_key,
        payload=json.dumps(payload.job_input, sort_keys=True) if payload.job_input is not None else None,
        correlation_id=payload.correlation_id,
        requested_by_type=payload.requested_by_type,
        requested_by_id=payload.requested_by_id or (
            str(payload.job_input.get("user_id"))
            if payload.job_input is not None and payload.job_input.get("user_id") is not None
            else None
        ),
        requested_by_display=payload.requested_by_display,
        priority=payload.priority,
        queued_at=now_bogota_iso(),
        status=JobExecutionStatus.QUEUED,
    )

    await execution_repo.add(execution)
    await publish_execution_update(execution)
    return execution


async def list_job_executions(
    execution_repo: Repository[JobExecution, UUID],
    *,
    client_key: str | None = None,
    job_key: str | None = None,
    requested_by_id: str | None = None,
    requested_by: str | None = None,
    statuses: list[str] | None = None,
    page: int = 1,
    page_size: int = 10,
) -> tuple[list[JobExecution], int]:
    filters: dict[str, str] = {}
    if client_key is not None:
        filters["client_key"] = client_key
    if job_key is not None:
        filters["job_key"] = job_key
    if requested_by_id is not None and client_key is None and job_key is None:
        filters["requested_by_id"] = requested_by_id

    if filters:
        executions = await execution_repo.list_by_fields(filters, limit=100)
    else:
        executions = await execution_repo.list(limit=100)

    if client_key is not None:
        executions = [execution for execution in executions if execution.client_key == client_key]
    if job_key is not None:
        executions = [execution for execution in executions if execution.job_key == job_key]
    if requested_by_id is not None:
        matching_executions: list[JobExecution] = []
        for execution in executions:
            if execution.requested_by_id == requested_by_id:
                matching_executions.append(execution)
                continue
            if not execution.payload:
                continue
            try:
                payload = json.loads(execution.payload)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict) and payload.get("user_id") == requested_by_id:
                matching_executions.append(execution)
        executions = matching_executions
    if requested_by:
        requested_by_normalized = requested_by.strip().lower()
        matching_executions: list[JobExecution] = []
        for execution in executions:
            candidates: list[str | None] = [
                execution.requested_by_id,
                execution.requested_by_display,
            ]
            if execution.payload:
                try:
                    payload = json.loads(execution.payload)
                except json.JSONDecodeError:
                    payload = None
                if isinstance(payload, dict):
                    user_id = payload.get("user_id")
                    candidates.append(str(user_id) if user_id is not None else None)
            if any(
                isinstance(candidate, str) and requested_by_normalized in candidate.lower()
                for candidate in candidates
            ):
                matching_executions.append(execution)
        executions = matching_executions
    if statuses:
        normalized_statuses = {status.lower() for status in statuses}
        executions = [
            execution
            for execution in executions
            if str(
                execution.status.value if hasattr(execution.status, "value") else execution.status
            ).lower() in normalized_statuses
        ]

    ordered_executions = sorted(
        executions,
        key=lambda execution: (
            execution.created_at or "",
            str(execution.id),
        ),
        reverse=True,
    )
    total = len(ordered_executions)
    safe_page = max(page, 1)
    safe_page_size = max(page_size, 1)
    start = (safe_page - 1) * safe_page_size
    end = start + safe_page_size
    return ordered_executions[start:end], total


async def get_job_execution(
    execution_repo: Repository[JobExecution, UUID],
    job_id: UUID,
) -> JobExecution | None:
    return await execution_repo.get(job_id)


async def get_job_execution_by_id_or_worker_id(
    execution_repo: Repository[JobExecution, UUID],
    ref: str,
) -> JobExecution | None:
    try:
        return await execution_repo.get(UUID(ref))
    except ValueError:
        pass
    results = await execution_repo.list_by_fields({"worker_id": ref}, limit=1)
    if results:
        return results[0]
    results = await execution_repo.list_by_fields({"inngest_run_id": ref}, limit=1)
    return results[0] if results else None


async def update_job_execution(
    execution_repo: Repository[JobExecution, UUID],
    job_id: UUID,
    payload: JobExecutionUpdate,
) -> JobExecution:
    execution = await execution_repo.get(job_id)
    if execution is None:
        raise ValueError("job execution not found")

    values = payload.model_dump(exclude_none=True)
    status_value = values.get("status")
    if status_value is not None and not isinstance(status_value, JobExecutionStatus):
        status_value = JobExecutionStatus(status_value)
        values["status"] = status_value

    if status_value == JobExecutionStatus.RUNNING and not execution.started_at and "started_at" not in values:
        values["started_at"] = now_bogota_iso()
    if status_value in _TERMINAL_STATUSES and "finished_at" not in values:
        values["finished_at"] = now_bogota_iso()

    if "started_at" in values and values["started_at"] is not None:
        values["started_at"] = str(values["started_at"])
    if "finished_at" in values and values["finished_at"] is not None:
        values["finished_at"] = str(values["finished_at"])
    if "result" in values and values["result"] is not None and not isinstance(values["result"], str):
        values["result"] = json.dumps(values["result"], sort_keys=True)
    if values.get("inngest_run_id") and "inngest_run_url" not in values:
        values["inngest_run_url"] = _build_inngest_run_url(values["inngest_run_id"])
    for key, value in values.items():
        setattr(execution, key, value)

    execution.updated_at = now_bogota_iso()
    await execution_repo.update(execution)
    await publish_execution_update(execution)
    return execution
