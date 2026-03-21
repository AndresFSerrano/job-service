from __future__ import annotations

from typing import Generic, TypeVar
from uuid import UUID, uuid4

import httpx
import pytest
from httpx import ASGITransport, AsyncClient

from app.application.dto.job_client_dto import JobClientCreate, ServiceRegistrationCreate
from app.application.dto.job_execution_dto import JobExecutionCreate, JobExecutionUpdate
from app.application.dto.job_definition_dto import JobDefinitionCreate
from app.application.use_cases.job_client_use_cases import (
    register_job_client,
    register_service_manifest,
)
from app.application.use_cases.job_definition_use_cases import register_job_definition
from app.application.use_cases.job_execution_use_cases import create_job_execution, update_job_execution
from app.domain.entities.job_client import JobClient
from app.domain.entities.job_definition import JobDefinition
from app.domain.entities.enums.job_execution_status import JobExecutionStatus
from app.domain.security.authenticated_user import AuthenticatedUser
from app.domain.security.roles import Role
from app.infrastructure.security.authorization import get_current_user
from job_service_sdk.client import JobServiceClient

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
async def test_register_job_client_is_idempotent_by_client_key() -> None:
    repo = InMemoryRepo[JobClient]()

    created = await register_job_client(
        repo,
        JobClientCreate(
            client_key="store-manager",
            display_name="Store Manager",
            base_url="http://store-v1:8000",
        ),
    )
    updated = await register_job_client(
        repo,
        JobClientCreate(
            client_key="store-manager",
            display_name="Store Manager v2",
            base_url="http://store-v2:8000",
            healthcheck_url="http://store-v2:8000/health",
        ),
    )

    assert len(repo.items) == 1
    assert updated.id == created.id
    assert updated.base_url == "http://store-v2:8000"
    assert updated.healthcheck_url == "http://store-v2:8000/health"


@pytest.mark.asyncio
async def test_register_service_manifest_upserts_client_and_definitions() -> None:
    client_repo = InMemoryRepo[JobClient]()
    definition_repo = InMemoryRepo[JobDefinition]()

    client, definitions = await register_service_manifest(
        client_repo,
        definition_repo,
        ServiceRegistrationCreate(
            client_key="apistoremanagerv1",
            display_name="API Store Manager",
            base_url="http://api-store:8000",
            job_definitions=[
                JobDefinitionCreate(
                    client_key="ignored-by-service-registration",
                    job_key="sync-products",
                    display_name="Sincronizar productos",
                )
            ],
        ),
    )

    assert client.client_key == "apistoremanagerv1"
    assert client.base_url == "http://api-store:8000"
    assert len(definitions) == 1
    assert definitions[0].client_key == "apistoremanagerv1"
    assert definitions[0].job_key == "sync-products"


@pytest.mark.asyncio
async def test_register_job_client_serializes_metadata_dict() -> None:
    repo = InMemoryRepo[JobClient]()

    client = await register_job_client(
        repo,
        JobClientCreate(
            client_key="store-manager",
            display_name="Store Manager",
            base_url="http://store-v1:8000",
            metadata={"service_version": "0.1.0"},
        ),
    )

    assert client.metadata == '{"service_version": "0.1.0"}'


@pytest.mark.asyncio
async def test_create_job_execution_keeps_worker_unassigned_while_queued() -> None:
    execution_repo = InMemoryRepo()
    definition_repo = InMemoryRepo[JobDefinition]()
    definition = JobDefinition(
        client_key="apistoremanagerv1",
        job_key="sync-products",
        display_name="Sincronizar productos",
    )
    await definition_repo.add(definition)

    execution = await create_job_execution(
        execution_repo,
        definition_repo,
        JobExecutionCreate(
            job_key="sync-products",
            job_input={"page": 1},
        ),
    )

    assert execution.status == JobExecutionStatus.QUEUED
    assert execution.worker_id is None
    assert execution.inngest_run_id is None
    assert execution.inngest_run_url is None
    assert execution.queued_at is not None
    assert execution.queued_at.endswith("-05:00")


@pytest.mark.asyncio
async def test_create_job_execution_defaults_requested_by_id_from_job_input_user_id() -> None:
    execution_repo = InMemoryRepo()
    definition_repo = InMemoryRepo[JobDefinition]()
    definition = JobDefinition(
        client_key="apistoremanagerv1",
        job_key="export_key_status_history",
        display_name="Exportar historial de llaves a S3",
    )
    await definition_repo.add(definition)

    execution = await create_job_execution(
        execution_repo,
        definition_repo,
        JobExecutionCreate(
            job_key="export_key_status_history",
            job_input={"user_id": "andres.serrano"},
        ),
    )

    assert execution.requested_by_id == "andres.serrano"


def test_job_execution_create_allows_empty_job_input() -> None:
    execution = JobExecutionCreate(
        job_key="sync-products",
        job_input={},
    )

    assert execution.job_input == {}


@pytest.mark.asyncio
async def test_update_job_execution_builds_inngest_run_url_from_job_service_settings(monkeypatch) -> None:
    monkeypatch.setenv("INNGEST_BASE_URL", "http://inngest:8288")
    monkeypatch.setenv("INNGEST_PUBLIC_URL", "http://localhost:8288")

    from app.core.config import get_settings

    get_settings.cache_clear()
    try:
        execution_repo = InMemoryRepo()
        definition_repo = InMemoryRepo[JobDefinition]()
        definition = JobDefinition(
            client_key="apistoremanagerv1",
            job_key="sync-products",
            display_name="Sincronizar productos",
        )
        await definition_repo.add(definition)
        execution = await create_job_execution(
            execution_repo,
            definition_repo,
            JobExecutionCreate(
                job_key="sync-products",
                job_input={"page": 1},
            ),
        )

        updated = await update_job_execution(
            execution_repo,
            execution.id,
            JobExecutionUpdate(
                worker_id="inngest",
                inngest_run_id="run_123",
            ),
        )

        assert updated.inngest_run_url == "http://localhost:8288/run?runID=run_123"
    finally:
        get_settings.cache_clear()


@pytest.mark.asyncio
async def test_update_job_execution_sets_started_and_finished_timestamps() -> None:
    execution_repo = InMemoryRepo()
    definition_repo = InMemoryRepo[JobDefinition]()
    definition = JobDefinition(
        client_key="apistoremanagerv1",
        job_key="sync-products",
        display_name="Sincronizar productos",
    )
    await definition_repo.add(definition)
    execution = await create_job_execution(
        execution_repo,
        definition_repo,
        JobExecutionCreate(
            job_key="sync-products",
            job_input={"page": 1},
        ),
    )

    running = await update_job_execution(
        execution_repo,
        execution.id,
        JobExecutionUpdate(status=JobExecutionStatus.RUNNING),
    )
    completed = await update_job_execution(
        execution_repo,
        execution.id,
        JobExecutionUpdate(status=JobExecutionStatus.COMPLETED),
    )

    assert running.started_at is not None
    assert completed.finished_at is not None


@pytest.mark.asyncio
async def test_register_job_definition_rejects_global_duplicate_job_key() -> None:
    repo = InMemoryRepo[JobDefinition]()

    await register_job_definition(
        repo,
        JobDefinitionCreate(
            client_key="service-a",
            job_key="sync-products",
            display_name="Sync Products A",
        ),
    )

    with pytest.raises(ValueError, match="already registered"):
        await register_job_definition(
            repo,
            JobDefinitionCreate(
                client_key="service-b",
                job_key="sync-products",
                display_name="Sync Products B",
            ),
        )


def test_job_service_client_discovers_definition_before_creating_execution() -> None:
    definition_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/api/v1/job-definitions":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": str(definition_id),
                        "client_key": "apistoremanagerv1",
                        "job_key": "sync-products",
                        "display_name": "Sincronizar productos",
                        "version": 1,
                        "active": True,
                    }
                ],
            )
        if request.method == "POST" and request.url.path == "/api/v1/job-executions":
            payload = request.read().decode("utf-8")
            return httpx.Response(201, json={"id": str(uuid4()), "payload_seen": payload})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    http_client = httpx.Client(
        base_url="http://job-service.local",
        transport=httpx.MockTransport(handler),
        headers={"Content-Type": "application/json"},
    )
    client = JobServiceClient("http://job-service.local", http_client=http_client)

    response = client.create_execution(
        "sync-products",
        job_input={"page": 1},
    )

    assert UUID(response["id"])
    assert '"job_key":"sync-products"' in response["payload_seen"]
    assert '"job_input":{"page":1}' in response["payload_seen"]


@pytest.mark.asyncio
async def test_service_registration_route_returns_created_payload(monkeypatch) -> None:
    monkeypatch.setenv("REPO_DATABASE", "memory")

    from app.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/job-clients/service-registration",
            json={
                "client_key": "apistoremanagerv1",
                "display_name": "API Store Manager",
                "base_url": "http://api-store:8000",
                "job_definitions": [
                    {
                        "client_key": "ignored",
                        "job_key": "sync-products",
                        "display_name": "Sincronizar productos",
                    }
                ],
            },
        )

    assert response.status_code == 201, response.text
    body = response.json()
    assert body["client"]["client_key"] == "apistoremanagerv1"
    assert body["client"]["base_url"] == "http://api-store:8000"
    assert body["job_definitions"][0]["client_key"] == "apistoremanagerv1"


@pytest.mark.asyncio
async def test_checkpoint_route_updates_existing_execution(monkeypatch) -> None:
    monkeypatch.setenv("REPO_DATABASE", "memory")

    from app.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        registration_response = await client.post(
            "/api/v1/job-clients/service-registration",
            json={
                "client_key": "apistoremanagerv1",
                "display_name": "API Store Manager",
                "base_url": "http://api-store:8000",
                "job_definitions": [
                    {
                        "client_key": "ignored",
                        "job_key": "sync-products",
                        "display_name": "Sincronizar productos",
                    }
                ],
            },
        )
        assert registration_response.status_code == 201, registration_response.text

        execution_response = await client.post(
            "/api/v1/job-executions",
            json={
                "job_key": "sync-products",
                "job_input": {"page": 1},
            },
        )
        assert execution_response.status_code == 201, execution_response.text
        execution_id = execution_response.json()["id"]

        checkpoint_response = await client.post(
            f"/api/v1/job-executions/{execution_id}/checkpoint",
            json={
                "events": [
                    {
                        "event_type": "progress",
                        "message": "avance inicial",
                        "level": "info",
                    }
                ],
                "progress_current": 1,
                "progress_total": 3,
                "progress_label": "running",
            },
        )

    assert checkpoint_response.status_code == 200, checkpoint_response.text
    assert checkpoint_response.json() == {"events_created": 1}


@pytest.mark.asyncio
async def test_list_job_executions_filters_by_requested_by_id_and_returns_result(monkeypatch) -> None:
    monkeypatch.setenv("REPO_DATABASE", "memory")

    from app.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post(
            "/api/v1/job-clients/service-registration",
            json={
                "client_key": "apistoremanagerv1",
                "display_name": "API Store Manager",
                "base_url": "http://api-store:8000",
                "job_definitions": [
                    {
                        "client_key": "ignored",
                        "job_key": "export_key_status_history",
                        "display_name": "Exportar historial de llaves a S3",
                    }
                ],
            },
        )

        create_response = await client.post(
            "/api/v1/job-executions",
            json={
                "job_key": "export_key_status_history",
                "job_input": {"user_id": "andres.serrano"},
                "requested_by_id": "andres.serrano",
            },
        )
        execution_id = create_response.json()["id"]

        patch_response = await client.patch(
            f"/api/v1/job-executions/{execution_id}",
            json={
                "status": "completed",
                "result": {"presigned_url": "https://example.com/file.xlsx"},
                "result_summary": "ok",
            },
        )
        assert patch_response.status_code == 200, patch_response.text

        list_response = await client.get(
            "/api/v1/job-executions",
            params={
                "job_key": "export_key_status_history",
                "requested_by_id": "andres.serrano",
            },
        )

    assert list_response.status_code == 200, list_response.text
    body = list_response.json()
    assert body["total"] == 1
    assert len(body["items"]) == 1
    assert body["items"][0]["requested_by_id"] == "andres.serrano"
    assert body["items"][0]["result"]["presigned_url"] == "https://example.com/file.xlsx"


@pytest.mark.asyncio
async def test_list_job_executions_filters_by_job_input_user_id_when_requested_by_id_is_missing(monkeypatch) -> None:
    monkeypatch.setenv("REPO_DATABASE", "memory")

    from app.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post(
            "/api/v1/job-clients/service-registration",
            json={
                "client_key": "apistoremanagerv1",
                "display_name": "API Store Manager",
                "base_url": "http://api-store:8000",
                "job_definitions": [
                    {
                        "client_key": "ignored",
                        "job_key": "export_key_status_history",
                        "display_name": "Exportar historial de llaves a S3",
                    }
                ],
            },
        )

        create_response = await client.post(
            "/api/v1/job-executions",
            json={
                "job_key": "export_key_status_history",
                "job_input": {"user_id": "andres.serrano"},
            },
        )
        execution_id = create_response.json()["id"]

        await client.patch(
            f"/api/v1/job-executions/{execution_id}",
            json={
                "status": "completed",
                "result": {"total_exported": 4},
            },
        )

        list_response = await client.get(
            "/api/v1/job-executions",
            params={
                "job_key": "export_key_status_history",
                "requested_by_id": "andres.serrano",
            },
        )

    assert list_response.status_code == 200, list_response.text
    body = list_response.json()
    assert any(item["id"] == execution_id for item in body["items"])
    assert all(item["job_input"]["user_id"] == "andres.serrano" for item in body["items"])
    matching = next(item for item in body["items"] if item["id"] == execution_id)
    assert matching["result"]["total_exported"] == 4


@pytest.mark.asyncio
async def test_list_job_executions_filters_by_multiple_statuses_and_paginates(monkeypatch) -> None:
    monkeypatch.setenv("REPO_DATABASE", "memory")

    from app.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post(
            "/api/v1/job-clients/service-registration",
            json={
                "client_key": "apistoremanagerv1",
                "display_name": "API Store Manager",
                "base_url": "http://api-store:8000",
                "job_definitions": [
                    {
                        "client_key": "ignored",
                        "job_key": "export_key_status_history",
                        "display_name": "Exportar historial de llaves a S3",
                    }
                ],
            },
        )

        first = await client.post(
            "/api/v1/job-executions",
            json={
                "job_key": "export_key_status_history",
                "job_input": {"user_id": "andres.serrano"},
                "requested_by_id": "andres.serrano",
            },
        )
        second = await client.post(
            "/api/v1/job-executions",
            json={
                "job_key": "export_key_status_history",
                "job_input": {"user_id": "andres.serrano"},
                "requested_by_id": "andres.serrano",
            },
        )

        await client.patch(
            f"/api/v1/job-executions/{first.json()['id']}",
            json={"status": "completed", "result": {"total_exported": 2}},
        )
        await client.patch(
            f"/api/v1/job-executions/{second.json()['id']}",
            json={"status": "failed", "error_message": "boom"},
        )

        response = await client.get(
            "/api/v1/job-executions",
            params=[
                ("job_key", "export_key_status_history"),
                ("requested_by_id", "andres.serrano"),
                ("status", "completed"),
                ("status", "failed"),
                ("page", "1"),
                ("page_size", "1"),
            ],
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["total"] >= 2
    assert body["page"] == 1
    assert body["page_size"] == 1
    assert len(body["items"]) == 1


@pytest.mark.asyncio
async def test_list_job_executions_filters_by_requested_by_search(monkeypatch) -> None:
    monkeypatch.setenv("REPO_DATABASE", "memory")

    from app.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post(
            "/api/v1/job-clients/service-registration",
            json={
                "client_key": "apistoremanagerv1",
                "display_name": "API Store Manager",
                "base_url": "http://api-store:8000",
                "job_definitions": [
                    {
                        "client_key": "ignored",
                        "job_key": "export_key_status_history",
                        "display_name": "Exportar historial de llaves a S3",
                    }
                ],
            },
        )

        await client.post(
            "/api/v1/job-executions",
            json={
                "job_key": "export_key_status_history",
                "job_input": {"user_id": "admin_general.seed"},
                "requested_by_id": "admin_general.seed",
                "requested_by_display": "admin_general.seed@udea.edu.co",
            },
        )
        await client.post(
            "/api/v1/job-executions",
            json={
                "job_key": "export_key_status_history",
                "job_input": {"user_id": "andres.serrano"},
                "requested_by_id": "andres.serrano",
                "requested_by_display": "andres.serrano@udea.edu.co",
            },
        )

        response = await client.get(
            "/api/v1/job-executions",
            params={
                "job_key": "export_key_status_history",
                "requested_by": "admin_general",
            },
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["requested_by_id"] == "admin_general.seed"


@pytest.mark.asyncio
async def test_list_job_execution_requester_options_returns_distinct_users(monkeypatch) -> None:
    monkeypatch.setenv("REPO_DATABASE", "memory")

    from app.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post(
            "/api/v1/job-clients/service-registration",
            json={
                "client_key": "apistoremanagerv1",
                "display_name": "API Store Manager",
                "base_url": "http://api-store:8000",
                "job_definitions": [
                    {
                        "client_key": "ignored",
                        "job_key": "export_key_status_history",
                        "display_name": "Exportar historial de llaves a S3",
                    }
                ],
            },
        )

        await client.post(
            "/api/v1/job-executions",
            json={
                "job_key": "export_key_status_history",
                "job_input": {"user_id": "admin_general.seed"},
                "requested_by_id": "admin_general.seed",
                "requested_by_display": "admin_general.seed@udea.edu.co",
            },
        )
        await client.post(
            "/api/v1/job-executions",
            json={
                "job_key": "export_key_status_history",
                "job_input": {"user_id": "admin_almacen.seed"},
                "requested_by_id": "admin_almacen.seed",
                "requested_by_display": "admin_almacen.seed@udea.edu.co",
            },
        )

        response = await client.get(
            "/api/v1/job-executions/requester-options",
            params={"job_key": "export_key_status_history"},
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert {"value": "admin_general.seed", "label": "admin_general.seed@udea.edu.co"} in body
    assert {"value": "admin_almacen.seed", "label": "admin_almacen.seed@udea.edu.co"} in body


@pytest.mark.asyncio
async def test_create_job_execution_route_returns_409_when_definition_disallows_concurrency(monkeypatch) -> None:
    monkeypatch.setenv("REPO_DATABASE", "memory")

    from app.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        registration_response = await client.post(
            "/api/v1/job-clients/service-registration",
            json={
                "client_key": "sample-service",
                "display_name": "Sample Service",
                "base_url": "http://sample-service:8000",
                "job_definitions": [
                    {
                        "client_key": "ignored",
                        "job_key": "sample-job",
                        "display_name": "Sample Job",
                        "allow_concurrent": False,
                    }
                ],
            },
        )
        assert registration_response.status_code == 201, registration_response.text

        first_response = await client.post(
            "/api/v1/job-executions",
            json={"job_key": "sample-job", "job_input": {"page": 1}},
        )
        second_response = await client.post(
            "/api/v1/job-executions",
            json={"job_key": "sample-job", "job_input": {"page": 2}},
        )

    assert first_response.status_code == 201, first_response.text
    assert second_response.status_code == 409, second_response.text
    assert "no permite concurrencia" in second_response.json()["detail"]


@pytest.mark.asyncio
async def test_requester_options_route_returns_403_for_non_admin_user(monkeypatch) -> None:
    monkeypatch.setenv("REPO_DATABASE", "memory")

    from app.main import app

    app.dependency_overrides[get_current_user] = lambda: AuthenticatedUser.from_values(
        subject="1",
        username="auxiliar",
        email="auxiliar@udea.edu.co",
        roles=[Role.AUXILIAR_ALMACEN],
    )

    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/api/v1/job-executions/requester-options")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 403, response.text
