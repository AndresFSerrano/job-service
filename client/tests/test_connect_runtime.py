from types import SimpleNamespace

import pytest

from job_service.connect_runtime import (
    start_job_runtime_from_settings,
    stop_job_runtime_from_settings,
)


@pytest.mark.asyncio
async def test_start_job_runtime_from_settings_defaults_to_inngest(monkeypatch):
    called = {"start": False}

    async def fake_start(settings, *, definitions_module, max_concurrency=None):
        called["start"] = True
        return {"definitions_module": definitions_module}

    monkeypatch.setattr(
        "job_service.connect_runtime.start_inngest_connect_worker_from_settings",
        fake_start,
    )

    result = await start_job_runtime_from_settings(
        SimpleNamespace(service_name="sample-service"),
        definitions_module="sample.jobs_config",
    )

    assert called["start"] is True
    assert result == {"definitions_module": "sample.jobs_config"}


@pytest.mark.asyncio
async def test_stop_job_runtime_from_settings_defaults_to_inngest(monkeypatch):
    called = {"app_id": None}

    async def fake_stop(app_id: str):
        called["app_id"] = app_id

    monkeypatch.setattr("job_service.connect_runtime.stop_inngest_connect_worker", fake_stop)

    await stop_job_runtime_from_settings(
        SimpleNamespace(service_name="sample-service"),
    )

    assert called["app_id"] == "sample-service"
