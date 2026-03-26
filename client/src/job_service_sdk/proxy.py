from __future__ import annotations

from typing import Callable

import httpx
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse

try:
    from fastapi import APIRouter, HTTPException, status
except ImportError:
    raise ImportError("fastapi is required for the proxy module. Install it with: pip install fastapi")

_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0))
    return _client


def _forward_headers(request: Request) -> dict[str, str]:
    headers: dict[str, str] = {}
    if "authorization" in request.headers:
        headers["authorization"] = request.headers["authorization"]
    if "content-type" in request.headers:
        headers["content-type"] = request.headers["content-type"]
    return headers


def _build_response(response: httpx.Response) -> Response:
    return Response(
        content=response.content,
        status_code=response.status_code,
        headers={"content-type": response.headers.get("content-type", "application/json")},
    )


def build_job_proxy_router(
    job_service_url_resolver: Callable[[], str | None],
    *,
    prefix: str = "/job-proxy",
    tags: list[str] | None = None,
) -> APIRouter:
    """
    Creates a FastAPI router that proxies job-service endpoints.

    Args:
        job_service_url_resolver: callable that returns the job-service base URL.
            Returns None when the service is not configured.
        prefix: URL prefix for the proxy routes.
        tags: OpenAPI tags.
    """
    router = APIRouter(prefix=prefix, tags=tags or ["Job Proxy"])

    def _resolve_url() -> str:
        url = job_service_url_resolver()
        if not url:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Job service not configured")
        return url.rstrip("/")

    @router.get("/executions", summary="Proxy: listar ejecuciones")
    async def proxy_list_executions(request: Request) -> Response:
        base = _resolve_url()
        url = f"{base}/api/v1/job-executions"
        if request.url.query:
            url = f"{url}?{request.url.query}"
        response = await _get_client().get(url, headers=_forward_headers(request))
        return _build_response(response)

    @router.post("/executions", summary="Proxy: crear ejecución")
    async def proxy_create_execution(request: Request) -> Response:
        base = _resolve_url()
        url = f"{base}/api/v1/job-executions"
        body = await request.body()
        response = await _get_client().post(url, content=body, headers=_forward_headers(request))
        return _build_response(response)

    @router.get("/executions/status-options", summary="Proxy: opciones de estado")
    async def proxy_status_options(request: Request) -> Response:
        base = _resolve_url()
        url = f"{base}/api/v1/job-executions/status-options"
        response = await _get_client().get(url, headers=_forward_headers(request))
        return _build_response(response)

    @router.get("/executions/requester-options", summary="Proxy: opciones de solicitante")
    async def proxy_requester_options(request: Request) -> Response:
        base = _resolve_url()
        url = f"{base}/api/v1/job-executions/requester-options"
        if request.url.query:
            url = f"{url}?{request.url.query}"
        response = await _get_client().get(url, headers=_forward_headers(request))
        return _build_response(response)

    @router.get("/executions/{execution_id}", summary="Proxy: obtener ejecución")
    async def proxy_get_execution(execution_id: str, request: Request) -> Response:
        base = _resolve_url()
        url = f"{base}/api/v1/job-executions/{execution_id}"
        response = await _get_client().get(url, headers=_forward_headers(request))
        return _build_response(response)

    @router.get("/executions/{execution_id}/stream", summary="Proxy: SSE stream de ejecución")
    async def proxy_execution_stream(execution_id: str, request: Request) -> StreamingResponse:
        base = _resolve_url()
        url = f"{base}/api/v1/job-executions/{execution_id}/stream"
        headers = _forward_headers(request)

        async def _event_generator():
            async with _get_client().stream("GET", url, headers=headers) as response:
                if response.status_code != 200:
                    return
                async for line in response.aiter_lines():
                    if await request.is_disconnected():
                        break
                    yield f"{line}\n"

        return StreamingResponse(
            _event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return router
