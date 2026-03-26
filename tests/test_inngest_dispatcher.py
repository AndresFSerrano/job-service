from __future__ import annotations

from datetime import timedelta
from typing import Generic, TypeVar
from uuid import UUID

import pytest

from app.core.config import Settings
from app.core.time import now_bogota_iso, parse_iso_datetime
from app.domain.entities.enums.job_execution_status import JobExecutionStatus
from app.domain.entities.job_definition import JobDefinition
from app.domain.entities.job_event import JobEvent
from app.domain.entities.job_execution import JobExecution
from app.workers.inngest_dispatcher import InngestJobDispatcher

T = TypeVar("T")


class InMemoryRepo(Generic[T]):
    def __init__(self) -> None:
        self.items: list[T] = []

    async def add(self, entity: T) -> None:
        self.items.append(entity)

    async def list(self, limit: int = 100) -> list[T]:
        return self.items[:limit]

    async def list_by_fields(self, fields: dict, limit: int = 100) -> list[T]:
        matches: list[T] = []
        for item in self.items:
            if all(getattr(item, key) == value for key, value in fields.items()):
                matches.append(item)
        return matches[:limit]

    async def get(self, entity_id: object) -> T | None:
        for item in self.items:
            if getattr(item, "id") == entity_id:
                return item
        return None

    async def update(self, entity: T) -> None:
        for index, current in enumerate(self.items):
            if getattr(current, "id") == getattr(entity, "id"):
                self.items[index] = entity
                return
        raise AssertionError("entity not found")


def _old_timestamp(minutes: int) -> str:
    current = parse_iso_datetime(now_bogota_iso())
    assert current is not None
    return (current - timedelta(minutes=minutes)).isoformat()


def _build_dispatcher(
    execution_repo: InMemoryRepo[JobExecution],
    definition_repo: InMemoryRepo[JobDefinition],
    event_repo: InMemoryRepo[JobEvent],
) -> InngestJobDispatcher:
    return InngestJobDispatcher(
        Settings(
            job_dispatch_stale_timeout_seconds=300,
            job_dispatch_reconcile_interval_seconds=30,
        ),
        execution_repo=execution_repo,
        definition_repo=definition_repo,
        client_repo=InMemoryRepo(),
        event_repo=event_repo,
    )


@pytest.mark.asyncio
async def test_fail_stale_running_inngest_execution_without_run_id() -> None:
    execution_repo = InMemoryRepo[JobExecution]()
    definition_repo = InMemoryRepo[JobDefinition]()
    event_repo = InMemoryRepo[JobEvent]()
    definition = JobDefinition(
        client_key="sample-service",
        job_key="sample-job",
        display_name="Sample Job",
        execution_engine="inngest",
        execution_ref="sample.event",
    )
    await definition_repo.add(definition)
    execution = JobExecution(
        job_definition_id=definition.id,
        client_key="sample-service",
        job_key="sample-job",
        status=JobExecutionStatus.RUNNING,
        queued_at=_old_timestamp(6),
        started_at=_old_timestamp(6),
        updated_at=_old_timestamp(6),
    )
    await execution_repo.add(execution)
    dispatcher = _build_dispatcher(execution_repo, definition_repo, event_repo)

    await dispatcher._fail_stale_dispatches()

    updated = await execution_repo.get(execution.id)
    assert updated is not None
    assert updated.status == JobExecutionStatus.FAILED
    assert updated.error_code == "inngest_dispatch_timeout"
    assert updated.finished_at is not None
    assert len(event_repo.items) == 1
    assert event_repo.items[0].event_type == "dispatch_timeout"
    assert event_repo.items[0].level == "error"


@pytest.mark.asyncio
async def test_keep_running_execution_when_inngest_run_id_exists() -> None:
    execution_repo = InMemoryRepo[JobExecution]()
    definition_repo = InMemoryRepo[JobDefinition]()
    event_repo = InMemoryRepo[JobEvent]()
    definition = JobDefinition(
        client_key="sample-service",
        job_key="sample-job",
        display_name="Sample Job",
        execution_engine="inngest",
        execution_ref="sample.event",
    )
    await definition_repo.add(definition)
    execution = JobExecution(
        job_definition_id=definition.id,
        client_key="sample-service",
        job_key="sample-job",
        status=JobExecutionStatus.RUNNING,
        inngest_run_id="run-123",
        queued_at=_old_timestamp(6),
        started_at=_old_timestamp(6),
        updated_at=_old_timestamp(6),
    )
    await execution_repo.add(execution)
    dispatcher = _build_dispatcher(execution_repo, definition_repo, event_repo)

    await dispatcher._fail_stale_dispatches()

    updated = await execution_repo.get(execution.id)
    assert updated is not None
    assert updated.status == JobExecutionStatus.RUNNING
    assert updated.error_code is None
    assert len(event_repo.items) == 0
