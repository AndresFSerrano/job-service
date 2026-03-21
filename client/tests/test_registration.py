from types import SimpleNamespace
from uuid import uuid4

import pytest

from job_service_sdk.client import JobDefinitionConfig
from job_service_sdk.registration import (
    _providers,
    bootstrap_job_service_provider,
    build_service_client_config,
    initialize_job_service_provider_from_settings,
)


class StubJobServiceClient:
    def __init__(self, base_url: str, *, api_key: str | None = None):
        self.base_url = base_url
        self.api_key = api_key
        self.register_service_calls: list[tuple[object, list[JobDefinitionConfig]]] = []

    def register_service(self, client_config, definitions):
        self.register_service_calls.append((client_config, definitions))
        return {
            "job_definitions": [
                {
                    "id": str(uuid4()),
                    "client_key": definition.client_key,
                    "job_key": definition.job_key,
                    "display_name": definition.display_name,
                }
                for definition in definitions
            ]
        }


def test_build_service_client_config_normalizes_metadata_and_urls():
    config = build_service_client_config(
        client_key="sample-service",
        display_name="Sample Service",
        public_base_url="http://localhost:8000/",
        healthcheck_path="/api/v1/health",
        service_version="0.1.0",
        metadata={"env": "test"},
    )

    assert config.base_url == "http://localhost:8000"
    assert config.healthcheck_url == "http://localhost:8000/api/v1/health"
    assert config.metadata == {"service_version": "0.1.0", "env": "test"}


def test_bootstrap_job_service_provider_returns_none_when_optional_and_missing_url():
    provider = bootstrap_job_service_provider(
        job_service_url=None,
        job_service_required=False,
        public_base_url="http://localhost:8000",
        healthcheck_path="/api/v1/health",
        client_key="sample-service",
        display_name="Sample Service",
        service_version="0.1.0",
        definitions=[],
    )

    assert provider is None


def test_initialize_job_service_provider_from_settings_caches_provider(monkeypatch):
    _providers.clear()
    monkeypatch.setattr("job_service_sdk.registration.importlib.import_module", lambda module: None)
    monkeypatch.setattr(
        "job_service_sdk.registration.build_job_definition_configs",
        lambda client_key: [
            JobDefinitionConfig(
                client_key=client_key,
                job_key="sample_job",
                display_name="Sample Job",
            )
        ],
        raising=False,
    )

    settings = SimpleNamespace(
        service_name="sample-service",
        service_version="0.1.0",
        job_service_url="http://job-service:8001",
        job_service_api_key="secret",
        public_base_url="http://localhost:8000",
        healthcheck_path="/api/v1/health",
        job_service_required=True,
    )

    first = initialize_job_service_provider_from_settings(
        settings,
        definitions_module="sample.jobs_config",
        client_factory=StubJobServiceClient,
    )
    second = initialize_job_service_provider_from_settings(
        settings,
        definitions_module="sample.jobs_config",
        client_factory=StubJobServiceClient,
    )

    assert first is second
    assert first is not None
    assert first.client.base_url == "http://job-service:8001"
