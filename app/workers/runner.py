import asyncio
import logging
from logging import Logger
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from app.clients.job_service_client import JobServiceClient
from app.domain.entities.enums.job_execution_status import JobExecutionStatus
from app.workers.registry import handler_registry

logger: Logger = logging.getLogger(__name__)


class JobWorker:
    def __init__(self, client: JobServiceClient, poll_interval: float = 5.0):
        self._client = client
        self._poll_interval = poll_interval
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            await self._task

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            await self._poll()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._poll_interval)
            except asyncio.TimeoutError:
                continue

    async def _poll(self) -> None:
        executions = await asyncio.to_thread(
            self._client.list_executions,
            status=JobExecutionStatus.QUEUED.value,
            limit=5,
        )
        for execution in executions:
            await self._handle_execution(execution)

    async def _handle_execution(self, execution: Mapping[str, Any]) -> None:
        job_id = UUID(execution["id"])
        job_key = execution["job_key"]
        handler = handler_registry.get_handler(job_key)
        if handler is None:
            await asyncio.to_thread(
                self._client.fail_execution,
                job_id,
                error_message=f"No handler registered for {job_key}",
            )
            return

        await asyncio.to_thread(self._client.update_execution, job_id, status=JobExecutionStatus.RUNNING.value)
        try:
            result = await handler(job_id, execution.get("job_input"))
            await asyncio.to_thread(self._client.complete_execution, job_id, result=result)
            await asyncio.to_thread(
                self._client.add_event,
                job_id,
                "job_completed",
                f"Job {job_key} completed",
            )
        except Exception as exc:
            logger.exception("job %s failed", job_key)
            await asyncio.to_thread(
                self._client.fail_execution,
                job_id,
                error_message=str(exc),
            )
            await asyncio.to_thread(
                self._client.add_event,
                job_id,
                "job_failed",
                f"Job {job_key} failed",
                level="error",
                data={"error": str(exc)},
            )
