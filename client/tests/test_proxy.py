import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from job_service_sdk.proxy import build_job_proxy_router


def _build_app(job_service_url: str | None = "http://fake-job:8001") -> FastAPI:
    app = FastAPI()
    router = build_job_proxy_router(lambda: job_service_url, prefix="/api/v1/job-proxy")
    app.include_router(router)
    return app


def _fake_response(status_code=200, content=b'{"ok":true}', content_type="application/json"):
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = content
    resp.headers = {"content-type": content_type}
    return resp


@pytest.mark.asyncio
async def test_proxy_list_executions():
    app = _build_app()
    fake_client = AsyncMock()
    fake_client.is_closed = False
    fake_client.get = AsyncMock(return_value=_fake_response(content=b'{"items":[],"total":0}'))

    with patch("job_service_sdk.proxy._client", fake_client):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/api/v1/job-proxy/executions?job_key=test&page=1")

    assert response.status_code == 200
    call_url = fake_client.get.call_args[0][0]
    assert "fake-job:8001/api/v1/job-executions" in call_url
    assert "job_key=test" in call_url


@pytest.mark.asyncio
async def test_proxy_create_execution():
    app = _build_app()
    fake_client = AsyncMock()
    fake_client.is_closed = False
    fake_client.post = AsyncMock(return_value=_fake_response(status_code=201, content=b'{"id":"abc"}'))

    with patch("job_service_sdk.proxy._client", fake_client):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/job-proxy/executions",
                json={"job_key": "test"},
            )

    assert response.status_code == 201
    fake_client.post.assert_called_once()


@pytest.mark.asyncio
async def test_proxy_status_options():
    app = _build_app()
    fake_client = AsyncMock()
    fake_client.is_closed = False
    fake_client.get = AsyncMock(return_value=_fake_response(content=b'["queued","running"]'))

    with patch("job_service_sdk.proxy._client", fake_client):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/api/v1/job-proxy/executions/status-options")

    assert response.status_code == 200
    call_url = fake_client.get.call_args[0][0]
    assert "status-options" in call_url


@pytest.mark.asyncio
async def test_proxy_get_execution():
    app = _build_app()
    fake_client = AsyncMock()
    fake_client.is_closed = False
    fake_client.get = AsyncMock(return_value=_fake_response(content=b'{"id":"abc","status":"running"}'))

    with patch("job_service_sdk.proxy._client", fake_client):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/api/v1/job-proxy/executions/abc")

    assert response.status_code == 200
    call_url = fake_client.get.call_args[0][0]
    assert "/job-executions/abc" in call_url


@pytest.mark.asyncio
async def test_proxy_forwards_authorization_header():
    app = _build_app()
    fake_client = AsyncMock()
    fake_client.is_closed = False
    fake_client.get = AsyncMock(return_value=_fake_response())

    with patch("job_service_sdk.proxy._client", fake_client):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                "/api/v1/job-proxy/executions/status-options",
                headers={"authorization": "Bearer token123"},
            )

    assert response.status_code == 200
    call_headers = fake_client.get.call_args[1].get("headers", {})
    assert call_headers.get("authorization") == "Bearer token123"


@pytest.mark.asyncio
async def test_proxy_returns_503_when_not_configured():
    app = _build_app(job_service_url=None)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/job-proxy/executions/status-options")

    assert response.status_code == 503


@pytest.mark.asyncio
async def test_proxy_requester_options():
    app = _build_app()
    fake_client = AsyncMock()
    fake_client.is_closed = False
    fake_client.get = AsyncMock(return_value=_fake_response(content=b'["user1","user2"]'))

    with patch("job_service_sdk.proxy._client", fake_client):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/api/v1/job-proxy/executions/requester-options?job_key=test")

    assert response.status_code == 200
    call_url = fake_client.get.call_args[0][0]
    assert "requester-options" in call_url
