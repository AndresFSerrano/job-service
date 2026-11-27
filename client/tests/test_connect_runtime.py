import asyncio
import json
import logging
from types import SimpleNamespace

import httpx
import pytest
from inngest.connect import ConnectionState

from job_service_sdk.connect_runtime import (
    start_job_runtime_from_settings,
    stop_job_runtime_from_settings,
)

CONNECTION_ID = "01M8D5TPF6RVZDSC0G897YAK2B"
API_ORIGIN = "http://inngest.local:8288"


class FakeWorker:
    def __init__(self, state=ConnectionState.ACTIVE, connection_id=CONNECTION_ID, stops_on_close=True):
        self.state = state
        self.connection_id = connection_id
        self.stops_on_close = stops_on_close
        self.closed = False
        self._stopped = asyncio.Event()

    async def start(self):
        await self._stopped.wait()

    async def close(self, wait=False):
        self.closed = True
        if self.stops_on_close:
            self._stopped.set()

    def get_state(self):
        return self.state

    def get_connection_id(self):
        return self.connection_id


@pytest.fixture
def fast_supervisor(monkeypatch):
    from job_service_sdk import connect_runtime

    monkeypatch.setattr(connect_runtime, "RECONNECT_DELAY_SECONDS", 0)
    monkeypatch.setattr(connect_runtime, "HEALTH_CHECK_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(connect_runtime, "WORKER_CLOSE_TIMEOUT_SECONDS", 0.05)
    return connect_runtime


async def wait_until(condition, attempts=300):
    for _ in range(attempts):
        if condition():
            return True
        await asyncio.sleep(0.01)
    return condition()


async def stop_task(task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def count_reached(counter, expected):
    def reached():
        return counter["n"] >= expected

    return reached


def dev_server_client():
    return SimpleNamespace(is_production=False, api_origin=API_ORIGIN)


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

    class DroppingWorker:
        async def start(self):
            intentos["n"] += 1
            if intentos["n"] < 3:
                raise ConnectionError("se cayo")
            await asyncio.sleep(3600)

        async def close(self, wait=False):
            return None

    def build_worker():
        return DroppingWorker()

    tarea = asyncio.create_task(
        connect_runtime._keep_connected("sample", build_worker(), build_worker)
    )

    assert await wait_until(count_reached(intentos, 3))
    await stop_task(tarea)


@pytest.mark.asyncio
async def test_a_worker_stuck_outside_active_is_replaced(fast_supervisor, caplog):
    built = []
    stuck = FakeWorker(state=ConnectionState.CONNECTING)

    def build_worker():
        worker = FakeWorker()
        built.append(worker)
        return worker

    caplog.set_level(logging.WARNING, logger="job_service_sdk.connect_runtime")
    task = asyncio.create_task(fast_supervisor._keep_connected("sample", stuck, build_worker))
    try:
        assert await wait_until(built.__len__)
    finally:
        await stop_task(task)

    assert stuck.closed is True
    assert "is not connected (connection state is CONNECTING), reconnecting" in caplog.text


@pytest.mark.asyncio
async def test_an_active_worker_the_server_no_longer_knows_is_replaced(fast_supervisor, monkeypatch, caplog):
    built = []
    zombie = FakeWorker(connection_id="zombie")

    async def fake_probe(transport, api_origin, connection_id):
        if connection_id == "zombie":
            return False, "the Inngest server reports connection zombie as DISCONNECTING"
        return True, "the Inngest server reports connection fresh as READY"

    def build_worker():
        worker = FakeWorker(connection_id="fresh")
        built.append(worker)
        return worker

    monkeypatch.setattr(fast_supervisor, "_probe_worker_connection", fake_probe)
    caplog.set_level(logging.WARNING, logger="job_service_sdk.connect_runtime")
    task = asyncio.create_task(
        fast_supervisor._keep_connected("sample", zombie, build_worker, dev_server_client())
    )
    try:
        assert await wait_until(built.__len__)
        await asyncio.sleep(0.1)
    finally:
        await stop_task(task)

    assert zombie.closed is True
    assert len(built) == 1
    assert built[0].closed is False
    assert "reports connection zombie as DISCONNECTING), reconnecting" in caplog.text


@pytest.mark.asyncio
async def test_an_unreachable_server_does_not_force_a_reconnect(fast_supervisor, monkeypatch):
    built = []
    checks = {"n": 0}
    worker = FakeWorker()

    async def unreachable(transport, api_origin, connection_id):
        checks["n"] += 1
        return None, "Inngest API unreachable: ConnectError"

    def build_worker():
        built.append(FakeWorker())
        return built[-1]

    monkeypatch.setattr(fast_supervisor, "_probe_worker_connection", unreachable)
    task = asyncio.create_task(fast_supervisor._keep_connected("sample", worker, build_worker, dev_server_client()))
    try:
        assert await wait_until(count_reached(checks, 12))
    finally:
        await stop_task(task)

    assert worker.closed is False
    assert built == []


@pytest.mark.asyncio
async def test_only_consecutive_failures_force_a_reconnect(fast_supervisor, monkeypatch):
    built = []
    checks = {"n": 0}
    answers = [False, False, False, True, False, None, False, False, False]
    worker = FakeWorker()

    async def flaky(transport, api_origin, connection_id):
        checks["n"] += 1
        answer = answers[checks["n"] - 1] if checks["n"] <= len(answers) else True
        return answer, "scripted answer"

    def build_worker():
        built.append(FakeWorker())
        return built[-1]

    monkeypatch.setattr(fast_supervisor, "_probe_worker_connection", flaky)
    task = asyncio.create_task(fast_supervisor._keep_connected("sample", worker, build_worker, dev_server_client()))
    try:
        assert await wait_until(built.__len__)
    finally:
        await stop_task(task)

    assert checks["n"] == len(answers)
    assert worker.closed is True


@pytest.mark.asyncio
async def test_a_worker_that_does_not_stop_is_left_behind(fast_supervisor, caplog):
    built = []
    stubborn = FakeWorker(state=ConnectionState.RECONNECTING, stops_on_close=False)

    def build_worker():
        built.append(FakeWorker())
        return built[-1]

    caplog.set_level(logging.WARNING, logger="job_service_sdk.connect_runtime")
    task = asyncio.create_task(fast_supervisor._keep_connected("sample", stubborn, build_worker))
    try:
        assert await wait_until(built.__len__)
    finally:
        await stop_task(task)

    assert stubborn.closed is True
    assert "did not stop in time" in caplog.text


@pytest.mark.asyncio
async def test_cloud_mode_trusts_the_worker_state_without_asking_the_server():
    from job_service_sdk import connect_runtime

    def fail(request):
        raise AssertionError("cloud mode must not query the Dev Server API")

    result = await connect_runtime._check_worker_connection(
        FakeWorker(),
        SimpleNamespace(is_production=True, api_origin="https://api.inngest.com"),
        httpx.MockTransport(fail),
    )

    assert result[0] is True


def graphql_response(body, status_code=200, date="Fri, 27 Nov 2026 00:20:00 GMT"):
    def handler(request):
        assert request.url == httpx.URL(f"{API_ORIGIN}/v0/gql")
        assert json.loads(request.content)["variables"] == {"id": CONNECTION_ID}
        content = body if isinstance(body, bytes) else json.dumps(body).encode()
        return httpx.Response(status_code, content=content, headers={"date": date})

    return handler


def connection(status, last_heartbeat="2026-11-27T00:19:50.22Z"):
    return {"data": {"workerConnection": {"status": status, "lastHeartbeatAt": last_heartbeat}}}


def unreachable_server(request):
    raise httpx.ConnectError("connection refused", request=request)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("handler", "expected", "detail"),
    [
        (graphql_response(connection("READY")), True, "as READY"),
        (graphql_response(connection("READY", "2026-11-27T00:18:00Z")), False, "no heartbeat from connection"),
        (graphql_response(connection("DISCONNECTING")), False, "as DISCONNECTING"),
        (
            graphql_response(
                {
                    "errors": [{"message": "sql: no rows in result set", "path": ["workerConnection"]}],
                    "data": {"workerConnection": None},
                }
            ),
            False,
            "does not know connection",
        ),
        (graphql_response({"errors": [{"message": "Cannot query field"}]}, status_code=422), None, "answered 422"),
        (graphql_response({"errors": [{"message": "Cannot query field"}]}), None, "does not expose workerConnection"),
        (graphql_response(b"<html>"), None, "not JSON"),
        (unreachable_server, None, "unreachable"),
    ],
)
async def test_the_dev_server_probe_reads_the_connection_state(handler, expected, detail):
    from job_service_sdk import connect_runtime

    connected, reason = await connect_runtime._probe_worker_connection(
        httpx.MockTransport(handler),
        API_ORIGIN,
        CONNECTION_ID,
    )

    assert connected is expected
    assert detail in reason


@pytest.mark.asyncio
async def test_is_worker_connected_requires_a_live_and_active_worker():
    from job_service_sdk import connect_runtime

    assert connect_runtime.is_worker_connected("no-existe") is False

    async def dormir():
        await asyncio.sleep(3600)

    worker = FakeWorker()
    connect_runtime._supervisors["vivo"] = asyncio.create_task(dormir())
    connect_runtime._workers["vivo"] = worker
    try:
        assert connect_runtime.is_worker_connected("vivo") is True

        worker.state = ConnectionState.CONNECTING
        assert connect_runtime.is_worker_connected("vivo") is False

        worker.state = ConnectionState.ACTIVE
        connect_runtime._unconfirmed_workers.add("vivo")
        assert connect_runtime.is_worker_connected("vivo") is False
    finally:
        connect_runtime._unconfirmed_workers.discard("vivo")
        connect_runtime._workers.pop("vivo")
        await stop_task(connect_runtime._supervisors.pop("vivo"))


@pytest.mark.asyncio
async def test_a_server_verdict_turns_is_worker_connected_off_before_reconnecting(fast_supervisor, monkeypatch):
    from job_service_sdk import connect_runtime

    checks = {"n": 0}

    async def disconnected(transport, api_origin, connection_id):
        checks["n"] += 1
        return False, "the Inngest server does not know connection"

    monkeypatch.setattr(connect_runtime, "HEALTH_CHECK_FAILURES_BEFORE_RECONNECT", 1000)
    monkeypatch.setattr(connect_runtime, "_probe_worker_connection", disconnected)
    worker = FakeWorker()
    connect_runtime._workers["zombie"] = worker
    connect_runtime._supervisors["zombie"] = asyncio.create_task(
        connect_runtime._keep_connected("zombie", worker, FakeWorker, dev_server_client())
    )
    try:
        assert await wait_until(count_reached(checks, 1))
        assert connect_runtime.is_worker_connected("zombie") is False
    finally:
        await connect_runtime.stop_inngest_connect_worker("zombie")

    assert "zombie" not in connect_runtime._unconfirmed_workers


@pytest.mark.asyncio
async def test_stopping_cancels_the_supervisor(monkeypatch):
    from job_service_sdk import connect_runtime

    cerrado = {"si": False}

    class ClosingWorker:
        async def close(self, wait=False):
            cerrado["si"] = True

    async def dormir():
        await asyncio.sleep(3600)

    connect_runtime._supervisors["sample"] = asyncio.create_task(dormir())
    connect_runtime._workers["sample"] = ClosingWorker()

    await connect_runtime.stop_inngest_connect_worker("sample")

    assert cerrado["si"] is True
    assert connect_runtime.is_worker_connected("sample") is False
