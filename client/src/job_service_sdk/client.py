from dataclasses import asdict, dataclass
from typing import Any, Callable, Mapping
from uuid import UUID

import httpx
from jsonschema import ValidationError as JsonSchemaValidationError, validate


class JobServiceError(RuntimeError):
    pass


HTTPClient = httpx.Client


@dataclass(slots=True)
class JobDefinitionConfig:
    client_key: str
    job_key: str
    display_name: str
    version: int = 1
    description: str | None = None
    payload_schema: dict[str, Any] | None = None
    result_schema: dict[str, Any] | None = None
    visibility_scope: str | None = None
    retry_policy: dict[str, Any] | None = None
    execution_engine: str = "local"
    execution_ref: str | None = None
    active: bool = True
    allow_concurrent: bool = True


@dataclass(slots=True)
class RegisteredJobDefinition:
    id: UUID
    config: JobDefinitionConfig


@dataclass(slots=True)
class JobClientConfig:
    client_key: str
    display_name: str
    base_url: str
    description: str | None = None
    status: str = "active"
    callback_auth_mode: str | None = None
    healthcheck_url: str | None = None
    metadata: dict[str, Any] | str | None = None


class JobServiceClient:
    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        timeout: float = 15.0,
        http_client: HTTPClient | None = None,
    ) -> None:
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        normalized_base_url = base_url.rstrip("/") + "/"
        self._client = http_client or HTTPClient(base_url=normalized_base_url, timeout=timeout, headers=headers)
        self._definitions: dict[tuple[str, str], RegisteredJobDefinition] = {}

    def register_client(self, client: JobClientConfig) -> dict[str, Any]:
        return self._request("POST", "/api/v1/job-clients", json=asdict(client))

    def register_definition(self, definition: JobDefinitionConfig) -> RegisteredJobDefinition:
        payload = asdict(definition)
        resp = self._request("POST", "/api/v1/job-definitions", json=payload)
        registered = RegisteredJobDefinition(id=UUID(resp["id"]), config=definition)
        self._definitions[(definition.client_key, definition.job_key)] = registered
        return registered

    def register_service(
        self,
        client: JobClientConfig,
        definitions: list[JobDefinitionConfig],
    ) -> dict[str, Any]:
        payload = asdict(client)
        payload["job_definitions"] = [asdict(definition) for definition in definitions]
        response = self._request("POST", "/api/v1/job-clients/service-registration", json=payload)
        for raw_definition in response.get("job_definitions", []):
            self._cache_definition(raw_definition)
        return response

    def list_definitions(
        self,
        *,
        client_key: str | None = None,
        job_key: str | None = None,
    ) -> list[dict[str, Any]]:
        raw = self._request("GET", "/api/v1/job-definitions")
        definitions = raw if isinstance(raw, list) else []
        filtered = [
            definition
            for definition in definitions
            if (client_key is None or definition.get("client_key") == client_key)
            and (job_key is None or definition.get("job_key") == job_key)
        ]
        for definition in filtered:
            self._cache_definition(definition)
        return filtered

    def list_clients(
        self,
        *,
        client_key: str | None = None,
    ) -> list[dict[str, Any]]:
        raw = self._request("GET", "/api/v1/job-clients")
        clients = raw if isinstance(raw, list) else []
        if client_key is None:
            return clients
        return [client for client in clients if client.get("client_key") == client_key]

    def create_execution(
        self,
        job_key: str,
        job_input: Mapping[str, Any] | None = None,
        *,
        correlation_id: str | None = None,
        requested_by_type: str = "user",
        requested_by_id: str | None = None,
        requested_by_display: str | None = None,
        priority: int = 0,
    ) -> dict[str, Any]:
        payload_body = {
            "job_key": job_key,
            "job_input": job_input,
            "correlation_id": correlation_id,
            "requested_by_type": requested_by_type,
            "requested_by_id": requested_by_id,
            "requested_by_display": requested_by_display,
            "priority": priority,
        }
        return self._request("POST", "/api/v1/job-executions", json=payload_body)

    def get_execution(self, ref: str | UUID) -> dict[str, Any]:
        return self._request("GET", f"/api/v1/job-executions/{ref}")

    def list_executions(
        self,
        status: str | None = None,
        job_key: str | None = None,
        requested_by_id: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        params = {
            "job_key": job_key,
            "requested_by_id": requested_by_id,
        }
        params = {key: value for key, value in params.items() if value is not None}
        raw = self._request("GET", "/api/v1/job-executions", params=params or None)
        executions = raw if isinstance(raw, list) else []
        filtered = [
            exec_
            for exec_ in executions
            if (status is None or exec_.get("status") == status)
            and (job_key is None or exec_.get("job_key") == job_key)
            and (requested_by_id is None or exec_.get("requested_by_id") == requested_by_id)
        ]
        if limit is not None:
            filtered = filtered[:limit]
        return filtered

    def report_progress(
        self,
        job_id: UUID,
        *,
        progress_current: int | None = None,
        progress_total: int | None = None,
        progress_label: str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "progress_current": progress_current,
            "progress_total": progress_total,
            "progress_label": progress_label,
        }
        return self.update_execution(job_id, **{k: v for k, v in payload.items() if v is not None})

    def update_execution(self, job_id: UUID, **kwargs: Any) -> dict[str, Any]:
        payload = {k: v for k, v in kwargs.items() if v is not None}
        if not payload:
            return {}
        return self._request("PATCH", f"/api/v1/job-executions/{job_id}", json=payload)

    def complete_execution(
        self,
        job_id: UUID,
        result: Mapping[str, Any] | None = None,
        *,
        result_schema: dict[str, Any] | None = None,
        result_summary: str | None = None,
    ) -> dict[str, Any]:
        if result_schema:
            self._validate_schema(result_schema, result or {}, "result")
        payload: dict[str, Any] = {"status": "completed"}
        if result_summary:
            payload["result_summary"] = result_summary
        if result is not None:
            payload["result"] = result
        return self._request("PATCH", f"/api/v1/job-executions/{job_id}", json=payload)

    def fail_execution(
        self,
        job_id: UUID,
        *,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> dict[str, Any]:
        payload = {"status": "failed", "error_code": error_code, "error_message": error_message}
        payload = {k: v for k, v in payload.items() if v is not None}
        return self._request("PATCH", f"/api/v1/job-executions/{job_id}", json=payload)

    def add_event(
        self,
        job_id: UUID,
        event_type: str,
        message: str,
        *,
        level: str = "info",
        data: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {"event_type": event_type, "message": message, "level": level, "data": data}
        return self._request("POST", f"/api/v1/job-executions/{job_id}/events", json=payload)

    def checkpoint(
        self,
        job_id: UUID,
        *,
        events: list[dict[str, Any]] | None = None,
        progress_current: int | None = None,
        progress_total: int | None = None,
        progress_label: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"events": events or []}
        if progress_current is not None:
            payload["progress_current"] = progress_current
        if progress_total is not None:
            payload["progress_total"] = progress_total
        if progress_label is not None:
            payload["progress_label"] = progress_label
        return self._request("POST", f"/api/v1/job-executions/{job_id}/checkpoint", json=payload)

    def run_once(
        self,
        handler: Callable[[dict[str, Any]], dict[str, Any]],
        *,
        job_key: str | None = None,
    ) -> None:
        executions = self.list_executions(status="queued", job_key=job_key, limit=1)
        if not executions:
            return
        execution = executions[0]
        job_id = UUID(execution["id"])
        job_input = execution.get("job_input") or {}
        registered = self._get_definition(execution["client_key"], execution["job_key"])

        if registered.config.payload_schema:
            self._validate_schema(registered.config.payload_schema, job_input, "job_input")

        self.report_progress(job_id, progress_label="started", progress_current=0)
        result = handler(job_input)
        if registered.config.result_schema:
            self._validate_schema(registered.config.result_schema, result, "result")
        self.complete_execution(job_id, result, result_schema=registered.config.result_schema)

    def _get_definition(self, client_key: str, job_key: str) -> RegisteredJobDefinition:
        key = (client_key, job_key)
        definition = self._definitions.get(key)
        if definition is None:
            discovered = self.list_definitions(client_key=client_key, job_key=job_key)
            if not discovered:
                raise JobServiceError(f"Definition not registered for {client_key}:{job_key}")
            definition = self._definitions.get(key)
        if definition is None:
            raise JobServiceError(f"Definition not registered for {client_key}:{job_key}")
        return definition

    def _cache_definition(self, raw_definition: Mapping[str, Any]) -> RegisteredJobDefinition:
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
        registered = RegisteredJobDefinition(id=UUID(str(raw_definition["id"])), config=config)
        self._definitions[(config.client_key, config.job_key)] = registered
        return registered

    def _validate_schema(self, schema: Mapping[str, Any], payload: Mapping[str, Any], label: str) -> None:
        try:
            validate(payload, schema)
        except JsonSchemaValidationError as exc:
            raise JobServiceError(f"{label} does not match schema: {exc.message}") from exc

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            response = self._client.request(method, path.lstrip("/"), **kwargs)
        except httpx.HTTPError as exc:
            raise JobServiceError(f"{method} {path} failed: {exc}") from exc
        if response.status_code >= 400:
            raise JobServiceError(f"{method} {path} failed ({response.status_code}): {response.text}")
        if response.text:
            try:
                return response.json()
            except ValueError:
                return response.text
        return {}
