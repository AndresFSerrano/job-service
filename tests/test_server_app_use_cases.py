from __future__ import annotations

import json
from typing import Generic, TypeVar
from uuid import UUID

import pytest

from app.application.dto.job_definition_dto import JobDefinitionCreate
from app.application.dto.job_event_dto import JobCheckpointCreate, JobEventCreate
from app.application.dto.job_execution_dto import JobExecutionCreate, JobExecutionUpdate
from app.application.use_cases.job_definition_use_cases import register_job_definition
from app.application.use_cases.job_event_use_cases import checkpoint_job_execution
from app.application.use_cases.job_execution_use_cases import (
    create_job_execution,
    list_job_execution_requesters,
    update_job_execution,
)
from app.domain.entities.enums.job_execution_status import JobExecutionStatus
from app.domain.entities.job_definition import JobDefinition
from app.domain.entities.job_event import JobEvent
from app.domain.entities.job_execution import JobExecution

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


@pytest.mark.asyncio
async def test_register_job_definition_serializes_schema_payloads_and_retry_policy() -> None:
    repo = InMemoryRepo[JobDefinition]()

    definition = await register_job_definition(
        repo,
        JobDefinitionCreate(
            client_key="sample-service",
            job_key="sample-job",
            display_name="Sample Job",
            payload_schema={"type": "object", "properties": {"page": {"type": "integer"}}},
            result_schema={"type": "object", "properties": {"ok": {"type": "boolean"}}},
            retry_policy={"max_attempts": 3},
        ),
    )

    assert json.loads(definition.payload_schema or "{}")["properties"]["page"]["type"] == "integer"
    assert json.loads(definition.result_schema or "{}")["properties"]["ok"]["type"] == "boolean"
    assert json.loads(definition.retry_policy or "{}")["max_attempts"] == 3


@pytest.mark.asyncio
async def test_create_job_execution_reuses_existing_correlation_id() -> None:
    execution_repo = InMemoryRepo[JobExecution]()
    definition_repo = InMemoryRepo[JobDefinition]()
    await definition_repo.add(
        JobDefinition(
            client_key="sample-service",
            job_key="sample-job",
            display_name="Sample Job",
        )
    )

    first = await create_job_execution(
        execution_repo,
        definition_repo,
        JobExecutionCreate(
            job_key="sample-job",
            job_input={"page": 1},
            correlation_id="corr-123",
        ),
    )
    second = await create_job_execution(
        execution_repo,
        definition_repo,
        JobExecutionCreate(
            job_key="sample-job",
            job_input={"page": 2},
            correlation_id="corr-123",
        ),
    )

    assert first.id == second.id
    assert len(execution_repo.items) == 1


@pytest.mark.asyncio
async def test_create_job_execution_rejects_active_duplicate_when_definition_disallows_concurrency() -> None:
    execution_repo = InMemoryRepo[JobExecution]()
    definition_repo = InMemoryRepo[JobDefinition]()
    await definition_repo.add(
        JobDefinition(
            client_key="sample-service",
            job_key="sample-job",
            display_name="Sample Job",
            allow_concurrent=False,
        )
    )

    first = await create_job_execution(
        execution_repo,
        definition_repo,
        JobExecutionCreate(
            job_key="sample-job",
            job_input={"page": 1},
        ),
    )
    assert first.status == JobExecutionStatus.QUEUED

    with pytest.raises(ValueError, match="no permite concurrencia"):
        await create_job_execution(
            execution_repo,
            definition_repo,
            JobExecutionCreate(
                job_key="sample-job",
                job_input={"page": 2},
            ),
        )


@pytest.mark.asyncio
async def test_checkpoint_job_execution_persists_events_and_updates_progress() -> None:
    event_repo = InMemoryRepo[JobEvent]()
    execution_repo = InMemoryRepo[JobExecution]()
    execution = JobExecution(
        job_definition_id=UUID("00000000-0000-0000-0000-000000000001"),
        client_key="sample-service",
        job_key="sample-job",
        status=JobExecutionStatus.RUNNING,
    )
    await execution_repo.add(execution)

    created = await checkpoint_job_execution(
        event_repo,
        execution_repo,
        execution.id,
        JobCheckpointCreate(
            events=[
                JobEventCreate(
                    event_type="progress",
                    message="halfway",
                    data={"step": 1},
                )
            ],
            progress_current=1,
            progress_total=2,
            progress_label="running",
        ),
    )

    updated = await execution_repo.get(execution.id)
    assert created == 1
    assert updated is not None
    assert updated.progress_current == 1
    assert updated.progress_total == 2
    assert updated.progress_label == "running"
    assert len(event_repo.items) == 1
    assert json.loads(event_repo.items[0].data or "{}") == {"step": 1}


@pytest.mark.asyncio
async def test_list_job_execution_requesters_uses_payload_user_id_and_deduplicates() -> None:
    execution_repo = InMemoryRepo[JobExecution]()
    await execution_repo.add(
        JobExecution(
            job_definition_id=UUID("00000000-0000-0000-0000-000000000001"),
            client_key="sample-service",
            job_key="sample-job",
            payload=json.dumps({"user_id": "user.one"}),
            requested_by_id=None,
            requested_by_display=None,
        )
    )
    await execution_repo.add(
        JobExecution(
            job_definition_id=UUID("00000000-0000-0000-0000-000000000002"),
            client_key="sample-service",
            job_key="sample-job",
            payload=json.dumps({"user_id": "user.one"}),
            requested_by_id="user.one",
            requested_by_display="user.one@example.com",
        )
    )

    options = await list_job_execution_requesters(execution_repo, job_key="sample-job")

    assert options == [{"value": "user.one", "label": "user.one@example.com"}]


@pytest.mark.asyncio
async def test_update_job_execution_serializes_result_payload() -> None:
    execution_repo = InMemoryRepo[JobExecution]()
    execution = JobExecution(
        job_definition_id=UUID("00000000-0000-0000-0000-000000000001"),
        client_key="sample-service",
        job_key="sample-job",
        status=JobExecutionStatus.RUNNING,
    )
    await execution_repo.add(execution)

    updated = await update_job_execution(
        execution_repo,
        execution.id,
        JobExecutionUpdate(
            status=JobExecutionStatus.COMPLETED,
            result={"ok": True, "total": 2},
            result_summary="done",
        ),
    )

    assert json.loads(updated.result or "{}") == {"ok": True, "total": 2}
    assert updated.result_summary == "done"
    assert updated.finished_at is not None
