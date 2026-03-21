from __future__ import annotations

import json
import logging
from typing import Any
from uuid import UUID

from persistence_kit.repository_factory import get_repo

from app.application.dto.job_event_dto import JobEventCreate
from app.application.dto.job_execution_dto import JobExecutionUpdate
from app.application.use_cases.job_event_use_cases import add_job_event
from app.application.use_cases.job_execution_use_cases import (
    get_job_execution as get_job_execution_use_case,
    update_job_execution as update_job_execution_use_case,
)
from app.core.config import Settings
from app.core.time import now_bogota_iso
from app.domain.entities.enums.job_execution_status import JobExecutionStatus
from app.domain.entities.job_execution import JobExecution

logger = logging.getLogger(__name__)

_dispatcher: InngestJobDispatcher | None = None


class InngestJobDispatcher:
    def __init__(
        self,
        settings: Settings,
    ) -> None:
        self._inngest: Any | None = None
        self._app_id = settings.service_name
        self._execution_repo = get_repo("job_execution")
        self._definition_repo = get_repo("job_definition")
        self._client_repo = get_repo("job_client")
        self._event_repo = get_repo("job_event")

    async def start(self) -> None:
        if self._inngest is None:
            import inngest

            self._inngest = inngest.Inngest(app_id=self._app_id)

    async def stop(self) -> None:
        return None

    async def dispatch_execution(self, job_id: UUID) -> None:
        execution = await get_job_execution_use_case(self._execution_repo, job_id)
        if execution is None:
            logger.warning("No se encontró ejecución %s para despacho", job_id)
            return
        await self._dispatch(execution)

    async def _dispatch(self, execution: JobExecution) -> None:
        if execution.status != JobExecutionStatus.QUEUED:
            return
        execution_endpoint = await self._resolve_execution_endpoint(execution.client_key)
        definitions = await self._definition_repo.list_by_fields(
            {"client_key": execution.client_key, "job_key": execution.job_key},
            limit=1,
        )
        if not definitions:
            logger.warning("No se encontró definición para job %s", execution.id)
            return

        definition = definitions[0]
        execution_engine = definition.execution_engine
        if execution_engine != "inngest":
            return

        execution_ref = definition.execution_ref
        if not execution_ref:
            logger.warning("Job %s no tiene execution_ref para Inngest", execution.id)
            return

        job_id = execution.id
        job_key = execution.job_key

        try:
            await update_job_execution_use_case(
                self._execution_repo,
                job_id,
                JobExecutionUpdate(
                    status=JobExecutionStatus.RUNNING,
                    started_at=execution.started_at or now_bogota_iso(),
                    progress_label="workflow_dispatched",
                ),
            )
            if self._inngest is None:
                raise RuntimeError("Inngest dispatcher no inicializado")
            import inngest

            await self._inngest.send(
                inngest.Event(
                    name=str(execution_ref),
                    id=f"job-{job_id}",
                    data={
                        "job_id": str(job_id),
                        "job_key": job_key,
                        "job_input": json.loads(execution.payload) if execution.payload else {},
                        "job_metadata": {
                            "client_key": execution.client_key,
                            "queued_at": execution.queued_at,
                            "requested_by_type": execution.requested_by_type,
                            "requested_by_id": execution.requested_by_id,
                            "requested_by_display": execution.requested_by_display,
                        },
                    },
                )
            )
            await add_job_event(
                self._event_repo,
                job_id,
                JobEventCreate(
                    event_type="workflow_dispatched",
                    message="Job enviado a Inngest",
                    data={
                        "job_key": job_key,
                        "engine": "inngest",
                        "execution_endpoint": execution_endpoint,
                    },
                ),
            )
        except Exception as exc:
            logger.exception("No se pudo despachar el job %s", job_id)
            await update_job_execution_use_case(
                self._execution_repo,
                job_id,
                JobExecutionUpdate(
                    status=JobExecutionStatus.FAILED,
                    error_message=f"No se pudo despachar a Inngest: {exc}",
                ),
            )

    async def _resolve_execution_endpoint(self, client_key: str) -> str | None:
        clients = await self._client_repo.list_by_fields({"client_key": client_key}, limit=1)
        if not clients:
            return None
        client = clients[0]
        base_url = str(client.base_url or "").rstrip("/")
        if not base_url:
            return None
        metadata = client.metadata
        if metadata:
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = None
        if not isinstance(metadata, dict):
            metadata = {}
        serve_path = str(metadata.get("inngest_serve_path") or "/api/inngest")
        if not serve_path.startswith("/"):
            serve_path = f"/{serve_path}"
        return f"{base_url}{serve_path}"


def set_dispatcher(dispatcher: InngestJobDispatcher | None) -> None:
    global _dispatcher
    _dispatcher = dispatcher


def get_dispatcher() -> InngestJobDispatcher | None:
    return _dispatcher
