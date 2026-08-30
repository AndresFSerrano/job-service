import asyncio
from types import SimpleNamespace

import pytest

from job_service_sdk.connect_runtime import (
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
        "job_service_sdk.connect_runtime.start_inngest_connect_worker_from_settings",
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

    monkeypatch.setattr("job_service_sdk.connect_runtime.stop_inngest_connect_worker", fake_stop)

    await stop_job_runtime_from_settings(
        SimpleNamespace(service_name="sample-service"),
    )

    assert called["app_id"] == "sample-service"


@pytest.mark.asyncio
async def test_the_worker_reconnects_when_the_connection_drops(monkeypatch):
    from job_service_sdk import connect_runtime

    monkeypatch.setattr(connect_runtime, "RECONNECT_DELAY_SECONDS", 0)
    intentos = {"n": 0}

    class FakeWorker:
        def __init__(self):
            self.started = False

        async def start(self):
            self.started = True
            intentos["n"] += 1
            if intentos["n"] < 3:
                raise ConnectionError("se cayo")
            await asyncio.sleep(3600)

        async def close(self, wait=False):
            return None

    def build_worker():
        return FakeWorker()

    tarea = asyncio.create_task(
        connect_runtime._keep_connected("sample", build_worker(), build_worker)
    )
    for _ in range(40):
        if intentos["n"] >= 3:
            break
        await asyncio.sleep(0)

    assert intentos["n"] >= 3
    tarea.cancel()


@pytest.mark.asyncio
async def test_is_worker_connected_reports_the_supervisor_state():
    from job_service_sdk import connect_runtime

    assert connect_runtime.is_worker_connected("no-existe") is False

    async def dormir():
        await asyncio.sleep(3600)

    connect_runtime._supervisors["vivo"] = asyncio.create_task(dormir())
    try:
        assert connect_runtime.is_worker_connected("vivo") is True
    finally:
        connect_runtime._supervisors.pop("vivo").cancel()


@pytest.mark.asyncio
async def test_stopping_cancels_the_supervisor(monkeypatch):
    from job_service_sdk import connect_runtime

    cerrado = {"si": False}

    class FakeWorker:
        async def close(self, wait=False):
            cerrado["si"] = True

    async def dormir():
        await asyncio.sleep(3600)

    connect_runtime._supervisors["sample"] = asyncio.create_task(dormir())
    connect_runtime._workers["sample"] = FakeWorker()

    await connect_runtime.stop_inngest_connect_worker("sample")

    assert cerrado["si"] is True
    assert connect_runtime.is_worker_connected("sample") is False
