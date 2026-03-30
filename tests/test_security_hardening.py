import pytest
from httpx import ASGITransport, AsyncClient

from app.core.config import (
    AuthProvider,
    DeploymentStage,
    LOCAL_DEFAULT_JOB_SERVICE_API_KEY,
    Settings,
    get_settings,
)


def test_local_stage_uses_default_job_service_api_key() -> None:
    settings = Settings()

    assert settings.job_service_api_key == LOCAL_DEFAULT_JOB_SERVICE_API_KEY


def test_settings_require_job_service_api_key_outside_local() -> None:
    with pytest.raises(ValueError, match="JOB_SERVICE_API_KEY"):
        Settings(
            stage=DeploymentStage.DEV,
            auth_enabled=True,
            auth_provider=AuthProvider.COGNITO,
            cognito_user_pool_id="pool-id",
            cognito_app_client_id="client-id",
            cors_origins=["https://jobs.example.edu.co"],
        )


def test_settings_reject_wildcard_cors_outside_local() -> None:
    with pytest.raises(ValueError, match="CORS_ORIGINS cannot contain '\\*'"):
        Settings(
            stage=DeploymentStage.PRODUCTION,
            auth_enabled=True,
            auth_provider=AuthProvider.COGNITO,
            cognito_user_pool_id="pool-id",
            cognito_app_client_id="client-id",
            cors_origins=["*"],
            job_service_api_key="service-secret",
        )


def test_create_app_disables_docs_outside_local(monkeypatch) -> None:
    monkeypatch.setenv("STAGE", "dev")
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("AUTH_PROVIDER", "cognito")
    monkeypatch.setenv("COGNITO_USER_POOL_ID", "pool-id")
    monkeypatch.setenv("COGNITO_APP_CLIENT_ID", "client-id")
    monkeypatch.setenv("CORS_ORIGINS", "https://jobs.example.edu.co")
    monkeypatch.setenv("JOB_SERVICE_API_KEY", "service-secret")

    from app.main import create_app

    get_settings.cache_clear()
    app = create_app()
    paths = {route.path for route in app.routes}

    assert "/docs" not in paths
    assert "/redoc" not in paths
    assert "/openapi.json" not in paths


@pytest.mark.asyncio
async def test_service_registration_requires_api_key_when_configured(monkeypatch) -> None:
    monkeypatch.setenv("REPO_DATABASE", "memory")
    monkeypatch.setenv("JOB_SERVICE_API_KEY", "service-secret")

    from app.main import create_app

    get_settings.cache_clear()
    app = create_app()
    transport = ASGITransport(app=app)
    payload = {
        "client_key": "sample-service",
        "display_name": "Sample Service",
        "base_url": "http://sample-service:8000",
        "job_definitions": [
            {
                "client_key": "ignored",
                "job_key": "sample-job",
                "display_name": "Sample Job",
            }
        ],
    }

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        unauthorized = await client.post("/api/v1/job-clients/service-registration", json=payload)
        authorized = await client.post(
            "/api/v1/job-clients/service-registration",
            json=payload,
            headers={"Authorization": "Bearer service-secret"},
        )

    assert unauthorized.status_code == 401, unauthorized.text
    assert authorized.status_code == 201, authorized.text


@pytest.mark.asyncio
async def test_service_api_key_can_create_and_list_executions(monkeypatch) -> None:
    monkeypatch.setenv("REPO_DATABASE", "memory")
    monkeypatch.setenv("JOB_SERVICE_API_KEY", "service-secret")

    from app.main import create_app

    get_settings.cache_clear()
    app = create_app()
    transport = ASGITransport(app=app)
    headers = {"Authorization": "Bearer service-secret"}
    registration_payload = {
        "client_key": "sample-service",
        "display_name": "Sample Service",
        "base_url": "http://sample-service:8000",
        "job_definitions": [
            {
                "client_key": "ignored",
                "job_key": "sample-job",
                "display_name": "Sample Job",
            }
        ],
    }

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        registration = await client.post(
            "/api/v1/job-clients/service-registration",
            json=registration_payload,
            headers=headers,
        )
        create_execution = await client.post(
            "/api/v1/job-executions",
            json={"job_key": "sample-job", "job_input": {"page": 1}},
            headers=headers,
        )
        list_executions = await client.get(
            "/api/v1/job-executions",
            params={"job_key": "sample-job"},
            headers=headers,
        )

    assert registration.status_code == 201, registration.text
    assert create_execution.status_code == 201, create_execution.text
    created_body = create_execution.json()
    assert created_body["requested_by_type"] == "service"
    assert created_body["requested_by_id"] == "job-service-internal"

    assert list_executions.status_code == 200, list_executions.text
    listed_body = list_executions.json()
    assert listed_body["total"] == 1
    assert listed_body["items"][0]["id"] == created_body["id"]
