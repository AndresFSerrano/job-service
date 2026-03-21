from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest

from job_service.client import JobDefinitionConfig, JobServiceClient, JobServiceError
from job_service.registration import (
    JobServiceProvider,
    bootstrap_job_service_provider,
    build_service_client_config,
)


def test_build_service_client_config_normalizes_base_url_and_metadata() -> None:
    config = build_service_client_config(
        client_key="api-store-manager",
        display_name="API Store Manager",
        public_base_url="http://api.example.com/",
        healthcheck_path="/api/v1/health",
        service_version="1.2.3",
        metadata={"region": "co"},
    )

    assert config.base_url == "http://api.example.com"
    assert config.healthcheck_url == "http://api.example.com/api/v1/health"
    assert config.metadata == {"service_version": "1.2.3", "region": "co"}


def test_bootstrap_job_service_provider_returns_none_when_remote_is_optional() -> None:
    provider = bootstrap_job_service_provider(
        job_service_url=None,
        job_service_required=False,
        public_base_url="http://api.example.com",
        healthcheck_path="/api/v1/health",
        client_key="api-store-manager",
        display_name="API Store Manager",
        service_version="1.0.0",
        definitions=[],
    )

    assert provider is None


def test_bootstrap_job_service_provider_requires_public_base_url_when_remote_enabled() -> None:
    with pytest.raises(RuntimeError, match="PUBLIC_BASE_URL"):
        bootstrap_job_service_provider(
            job_service_url="http://job-service.local",
            job_service_required=True,
            public_base_url=None,
            healthcheck_path="/api/v1/health",
            client_key="api-store-manager",
            display_name="API Store Manager",
            service_version="1.0.0",
            definitions=[],
        )


def test_job_service_client_create_execution_sends_expected_payload() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = request.content.decode("utf-8")
        return httpx.Response(201, json={"id": str(uuid4())})

    http_client = httpx.Client(
        base_url="http://job-service.local",
        transport=httpx.MockTransport(handler),
        headers={"Content-Type": "application/json"},
    )
    client = JobServiceClient("http://job-service.local", http_client=http_client)

    response = client.create_execution(
        "sample_job",
        job_input={"sample_key": "sample_value"},
        correlation_id="corr-123",
        requested_by_id="sample.user",
        requested_by_display="sample.user@example.com",
        priority=2,
    )

    assert UUID(response["id"])
    assert seen["path"] == "/api/v1/job-executions"
    body = str(seen["body"])
    assert '"job_key":"sample_job"' in body
    assert '"correlation_id":"corr-123"' in body
    assert '"requested_by_id":"sample.user"' in body


def test_job_service_client_complete_execution_validates_result_schema() -> None:
    client = JobServiceClient(
        "http://job-service.local",
        http_client=httpx.Client(
            base_url="http://job-service.local",
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json={})),
            headers={"Content-Type": "application/json"},
        ),
    )

    with pytest.raises(JobServiceError, match="result does not match schema"):
        client.complete_execution(
            uuid4(),
            result={"total_exported": "4"},
            result_schema={
                "type": "object",
                "properties": {"total_exported": {"type": "integer"}},
                "required": ["total_exported"],
            },
        )


def test_job_service_client_request_maps_http_errors() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(500, text="boom"))
    client = JobServiceClient(
        "http://job-service.local",
        http_client=httpx.Client(
            base_url="http://job-service.local",
            transport=transport,
            headers={"Content-Type": "application/json"},
        ),
    )

    with pytest.raises(JobServiceError, match="GET /api/v1/job-definitions failed \\(500\\): boom"):
        client.list_definitions()


def test_job_service_provider_rejects_unregistered_job_execution() -> None:
    provider = JobServiceProvider(
        client=SimpleNamespace(create_execution=lambda **kwargs: kwargs),  # type: ignore[arg-type]
        client_key="api-store-manager",
    )

    with pytest.raises(JobServiceError, match="no registrado"):
        provider.create_execution("sample_job", {"sample_key": "sample_value"})


def test_job_service_client_run_once_discovers_definition_and_completes_execution() -> None:
    definition_id = uuid4()
    execution_id = uuid4()
    seen_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(f"{request.method} {request.url.path}")
        if request.method == "GET" and request.url.path == "/api/v1/job-executions":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": str(execution_id),
                        "client_key": "api-store-manager",
                        "job_key": "sample_job",
                        "status": "queued",
                        "job_input": {"sample_key": "sample_value"},
                    }
                ],
            )
        if request.method == "GET" and request.url.path == "/api/v1/job-definitions":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": str(definition_id),
                        "client_key": "api-store-manager",
                        "job_key": "sample_job",
                        "display_name": "Sample Job",
                        "payload_schema": {
                            "type": "object",
                            "properties": {"sample_key": {"type": "string"}},
                            "required": ["sample_key"],
                        },
                        "result_schema": {
                            "type": "object",
                            "properties": {"processed_items": {"type": "integer"}},
                            "required": ["processed_items"],
                        },
                    }
                ],
            )
        if request.method == "PATCH" and request.url.path == f"/api/v1/job-executions/{execution_id}":
            return httpx.Response(200, json={"ok": True})
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    client = JobServiceClient(
        "http://job-service.local",
        http_client=httpx.Client(
            base_url="http://job-service.local",
            transport=httpx.MockTransport(handler),
            headers={"Content-Type": "application/json"},
        ),
    )

    client.run_once(lambda job_input: {"processed_items": 4}, job_key="sample_job")

    assert "GET /api/v1/job-definitions" in seen_paths
    assert f"PATCH /api/v1/job-executions/{execution_id}" in seen_paths
