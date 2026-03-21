from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import Any, Callable, Sequence

from job_service_sdk.client import JobClientConfig, JobDefinitionConfig, JobServiceClient, JobServiceError


@dataclass(slots=True)
class JobServiceProvider:
    client: JobServiceClient
    client_key: str
    _definitions: dict[str, JobDefinitionConfig] = field(default_factory=dict, init=False)

    def register_definition(self, config: JobDefinitionConfig) -> JobDefinitionConfig:
        if config.client_key != self.client_key:
            raise ValueError("JobDefinitionConfig client_key must match provider client_key")
        self.client.register_definition(config)
        self._definitions[config.job_key] = config
        return config

    def register_definitions(self, definitions: Sequence[JobDefinitionConfig]) -> None:
        for definition in definitions:
            self.register_definition(definition)

    def register_service(self, client_config: JobClientConfig, definitions: Sequence[JobDefinitionConfig]) -> None:
        response = self.client.register_service(client_config, list(definitions))
        self._definitions.clear()
        for raw_definition in response.get("job_definitions", []):
            config = JobDefinitionConfig(
                client_key=raw_definition["client_key"],
                job_key=raw_definition["job_key"],
                display_name=raw_definition["display_name"],
                version=raw_definition.get("version", 1),
                description=raw_definition.get("description"),
                payload_schema=raw_definition.get("payload_schema"),
                result_schema=raw_definition.get("result_schema"),
                visibility_scope=raw_definition.get("visibility_scope"),
                retry_policy=raw_definition.get("retry_policy"),
                execution_engine=raw_definition.get("execution_engine", "local"),
                execution_ref=raw_definition.get("execution_ref"),
                active=raw_definition.get("active", True),
            )
            self._definitions[config.job_key] = config

    def create_execution(
        self,
        job_key: str,
        job_input: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if job_key not in self._definitions:
            raise JobServiceError(f"job {job_key} no registrado")

        return self.client.create_execution(
            job_key=job_key,
            job_input=job_input,
            **kwargs,
        )


def build_service_client_config(
    *,
    client_key: str,
    display_name: str,
    public_base_url: str,
    healthcheck_path: str,
    service_version: str,
    metadata: dict[str, Any] | None = None,
) -> JobClientConfig:
    normalized_base_url = public_base_url.rstrip("/")
    combined_metadata = {"service_version": service_version}
    if metadata:
        combined_metadata.update(metadata)
    return JobClientConfig(
        client_key=client_key,
        display_name=display_name,
        base_url=normalized_base_url,
        healthcheck_url=f"{normalized_base_url}{healthcheck_path}",
        metadata=combined_metadata,
    )


def bootstrap_job_service_provider(
    *,
    job_service_url: str | None,
    job_service_required: bool,
    public_base_url: str | None,
    healthcheck_path: str,
    client_key: str,
    display_name: str,
    service_version: str,
    definitions: Sequence[JobDefinitionConfig],
    api_key: str | None = None,
    metadata: dict[str, Any] | None = None,
    client_factory: Callable[..., JobServiceClient] = JobServiceClient,
) -> JobServiceProvider | None:
    if not job_service_url:
        if job_service_required:
            raise RuntimeError("JOB_SERVICE_URL es obligatorio para iniciar el provider remoto de job_service")
        return None
    if not public_base_url:
        raise RuntimeError("PUBLIC_BASE_URL es obligatorio para registrar el servicio en job_service")

    client = client_factory(job_service_url, api_key=api_key)
    provider = JobServiceProvider(client=client, client_key=client_key)
    service_config = build_service_client_config(
        client_key=client_key,
        display_name=display_name,
        public_base_url=public_base_url,
        healthcheck_path=healthcheck_path,
        service_version=service_version,
        metadata=metadata,
    )
    provider.register_service(service_config, definitions)
    return provider


_providers: dict[str, JobServiceProvider] = {}


def initialize_job_service_provider_from_settings(
    settings: Any,
    *,
    definitions_module: str,
    metadata: dict[str, Any] | None = None,
    client_factory: Callable[..., JobServiceClient] = JobServiceClient,
) -> JobServiceProvider | None:
    existing = _providers.get(settings.service_name)
    if existing is not None:
        return existing

    importlib.import_module(definitions_module)
    from job_service_sdk.jobs import build_job_definition_configs

    provider = bootstrap_job_service_provider(
        job_service_url=getattr(settings, "job_service_url", None),
        job_service_required=getattr(settings, "job_service_required", True),
        public_base_url=getattr(settings, "public_base_url", None),
        healthcheck_path=getattr(settings, "healthcheck_path", "/api/v1/health"),
        client_key=settings.service_name,
        display_name=settings.service_name.replace("-", " ").title(),
        service_version=getattr(settings, "service_version", "0.1.0"),
        definitions=build_job_definition_configs(settings.service_name),
        api_key=getattr(settings, "job_service_api_key", None),
        metadata=metadata,
        client_factory=client_factory,
    )
    if provider is not None:
        _providers[settings.service_name] = provider
    return provider


def get_job_service_provider(client_key: str) -> JobServiceProvider | None:
    return _providers.get(client_key)
