import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Any
from uuid import UUID

from job_service_sdk.client import JobServiceClient

logger = logging.getLogger(__name__)

JobHandler = Callable[[UUID, Mapping[str, object] | None], Awaitable[Mapping[str, object]]]


class JobHandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, JobHandler] = {}

    def register(self, job_key: str, handler: JobHandler) -> None:
        self._handlers[job_key] = handler

    def get_handler(self, job_key: str) -> JobHandler | None:
        return self._handlers.get(job_key)


handler_registry = JobHandlerRegistry()


def handler(job_key: str) -> Callable[[JobHandler], JobHandler]:
    def decorator(func: JobHandler) -> JobHandler:
        handler_registry.register(job_key, func)
        return func

    return decorator


class JobWorker:
    def __init__(self, client: JobServiceClient, poll_interval: float = 5.0) -> None:
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
        executions = await asyncio.to_thread(self._client.list_executions, status="queued", limit=5)
        for execution in executions:
            await self._handle_execution(execution)

    async def _handle_execution(self, execution: Mapping[str, Any]) -> None:
        job_id = UUID(execution["id"])
        job_key = execution["job_key"]
        registered_handler = handler_registry.get_handler(job_key)
        if registered_handler is None:
            await asyncio.to_thread(
                self._client.fail_execution,
                job_id,
                error_message=f"handler no registrado para {job_key}",
            )
            return

        await asyncio.to_thread(self._client.update_execution, job_id, status="running")
        payload = execution.get("job_input")
        try:
            result = await registered_handler(job_id, payload)
            await asyncio.to_thread(
                self._client.complete_execution,
                job_id,
                result,
                result_summary=result.get("result_summary") if isinstance(result, Mapping) else None,
            )
            await asyncio.to_thread(
                self._client.add_event,
                job_id,
                "job_completed",
                "Job finalizado correctamente",
                data={"result_summary": result.get("result_summary")} if isinstance(result, Mapping) else None,
            )
        except Exception as exc:
            logger.exception("job %s fallo", job_key, exc_info=exc)
            await asyncio.to_thread(self._client.fail_execution, job_id, error_message=str(exc))
            await asyncio.to_thread(
                self._client.add_event,
                job_id,
                "job_failed",
                "Job fallo durante la ejecucion",
                level="error",
                data={"error": str(exc)},
            )
