from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID

from app.domain.entities.job_event import JobEvent
from app.domain.entities.job_execution import JobExecution


class JobExecutionStreamHub:
    def __init__(self) -> None:
        self._subscribers: dict[UUID, set[asyncio.Queue[dict[str, Any]]]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def subscribe(self, job_id: UUID) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        async with self._lock:
            self._subscribers[job_id].add(queue)
        return queue

    async def unsubscribe(self, job_id: UUID, queue: asyncio.Queue[dict[str, Any]]) -> None:
        async with self._lock:
            subscribers = self._subscribers.get(job_id)
            if not subscribers:
                return
            subscribers.discard(queue)
            if not subscribers:
                self._subscribers.pop(job_id, None)

    async def publish(self, job_id: UUID, event_name: str, data: dict[str, Any]) -> None:
        async with self._lock:
            subscribers = list(self._subscribers.get(job_id, set()))
        for queue in subscribers:
            await queue.put({"event": event_name, "data": data})


hub = JobExecutionStreamHub()


def serialize_job_execution(execution: JobExecution) -> dict[str, Any]:
    status = execution.status.value if hasattr(execution.status, "value") else str(execution.status)
    return {
        "id": str(execution.id),
        "job_definition_id": str(execution.job_definition_id),
        "client_key": execution.client_key,
        "job_key": execution.job_key,
        "job_input": json.loads(execution.payload) if execution.payload else None,
        "correlation_id": execution.correlation_id,
        "status": status,
        "requested_by_type": execution.requested_by_type,
        "requested_by_id": execution.requested_by_id,
        "requested_by_display": execution.requested_by_display,
        "priority": execution.priority,
        "progress_current": execution.progress_current,
        "progress_total": execution.progress_total,
        "progress_label": execution.progress_label,
        "result": json.loads(execution.result) if execution.result else None,
        "result_summary": execution.result_summary,
        "error_code": execution.error_code,
        "error_message": execution.error_message,
        "worker_id": execution.worker_id,
        "inngest_run_id": execution.inngest_run_id,
        "inngest_run_url": execution.inngest_run_url,
        "queued_at": execution.queued_at,
        "started_at": execution.started_at,
        "finished_at": execution.finished_at,
        "created_at": execution.created_at,
        "updated_at": execution.updated_at,
    }


def serialize_job_event(event: JobEvent) -> dict[str, Any]:
    return {
        "id": str(event.id),
        "job_execution_id": str(event.job_execution_id),
        "event_type": event.event_type,
        "message": event.message,
        "data": json.loads(event.data) if event.data else None,
        "level": event.level,
        "created_at": event.created_at,
    }


async def publish_execution_update(execution: JobExecution) -> None:
    await hub.publish(
        execution.id,
        "execution_updated",
        {"execution": serialize_job_execution(execution)},
    )


async def publish_job_event(event: JobEvent) -> None:
    await hub.publish(
        event.job_execution_id,
        "job_event_created",
        {"event": serialize_job_event(event)},
    )


def format_sse_message(event_name: str, data: dict[str, Any]) -> str:
    return f"event: {event_name}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


async def stream_events(
    job_id: UUID,
    *,
    initial_execution: JobExecution,
    ping_interval: float = 15.0,
) -> AsyncIterator[str]:
    queue = await hub.subscribe(job_id)
    try:
        yield format_sse_message(
            "execution_snapshot",
            {"execution": serialize_job_execution(initial_execution)},
        )
        while True:
            try:
                payload = await asyncio.wait_for(queue.get(), timeout=ping_interval)
            except asyncio.TimeoutError:
                yield format_sse_message("ping", {"job_id": str(job_id)})
                continue
            yield format_sse_message(payload["event"], payload["data"])
    finally:
        await hub.unsubscribe(job_id, queue)
