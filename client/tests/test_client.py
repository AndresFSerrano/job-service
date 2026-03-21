from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from job_service_sdk.client import (
    JobClientConfig,
    JobDefinitionConfig,
    JobServiceClient,
    JobServiceError,
)


class StubResponse:
    def __init__(self, *, status_code: int = 200, json_data=None, text: str | None = None):
        self.status_code = status_code
        self._json_data = json_data
        if text is not None:
            self.text = text
        elif json_data is None:
            self.text = ""
        else:
            self.text = "json"

    def json(self):
        if isinstance(self._json_data, Exception):
            raise self._json_data
        return self._json_data


class StubHTTPClient:
    def __init__(self, responses: list[StubResponse]):
        self._responses = responses
        self.calls: list[dict[str, object]] = []

    def request(self, method: str, path: str, **kwargs):
        self.calls.append({"method": method, "path": path, "kwargs": kwargs})
        return self._responses.pop(0)


def test_register_service_posts_client_and_definitions_payload():
    http_client = StubHTTPClient(
        [
            StubResponse(
                json_data={
                    "job_definitions": [
                        {
                            "id": str(uuid4()),
                            "client_key": "sample-backend",
                            "job_key": "sample_job",
                            "display_name": "Sample Job",
                        }
                    ]
                }
            )
        ]
    )
    client = JobServiceClient("http://job-service.test", http_client=http_client)

    client.register_service(
        JobClientConfig(
            client_key="sample-backend",
            display_name="Sample Backend",
            base_url="http://sample-backend:8000",
        ),
        [
            JobDefinitionConfig(
                client_key="sample-backend",
                job_key="sample_job",
                display_name="Sample Job",
            )
        ],
    )

    request = http_client.calls[0]
    assert request["method"] == "POST"
    assert request["path"] == "/api/v1/job-clients/service-registration"
    payload = request["kwargs"]["json"]
    assert payload["client_key"] == "sample-backend"
    assert payload["job_definitions"][0]["job_key"] == "sample_job"


def test_run_once_discovers_definition_and_completes_execution():
    execution_id = uuid4()
    discovered_definition_id = uuid4()
    http_client = StubHTTPClient(
        [
            StubResponse(
                json_data=[
                    {
                        "id": str(execution_id),
                        "client_key": "sample-backend",
                        "job_key": "sample_job",
                        "status": "queued",
                        "job_input": {"value": 2},
                    }
                ]
            ),
            StubResponse(
                json_data=[
                    {
                        "id": str(discovered_definition_id),
                        "client_key": "sample-backend",
                        "job_key": "sample_job",
                        "display_name": "Sample Job",
                    }
                ]
            ),
            StubResponse(json_data={"ok": True}),
            StubResponse(json_data={"ok": True}),
        ]
    )
    client = JobServiceClient("http://job-service.test", http_client=http_client)

    client.run_once(lambda payload: {"processed": payload["value"] + 1})

    patch_calls = [call for call in http_client.calls if call["method"] == "PATCH"]
    assert len(patch_calls) == 2
    progress_call = patch_calls[0]
    complete_call = patch_calls[1]
    assert progress_call["method"] == "PATCH"
    assert progress_call["path"] == f"/api/v1/job-executions/{execution_id}"
    assert complete_call["kwargs"]["json"]["status"] == "completed"
    assert complete_call["kwargs"]["json"]["result"] == {"processed": 3}


def test_request_wraps_http_error():
    class FailingHTTPClient:
        def request(self, method: str, path: str, **kwargs):
            raise httpx.ConnectError("network down", request=httpx.Request(method, f"http://x{path}"))

    client = JobServiceClient("http://job-service.test", http_client=FailingHTTPClient())

    with pytest.raises(JobServiceError, match="GET /api/v1/job-clients failed"):
        client.list_clients()
