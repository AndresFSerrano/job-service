from __future__ import annotations

import importlib
from typing import Any

import inngest

from job_service.jobs import build_inngest_functions
from job_service.registration import get_job_service_provider

_inngest_clients: dict[str, inngest.Inngest] = {}
_workers: dict[str, Any] = {}


def initialize_inngest_client_from_settings(settings: Any) -> inngest.Inngest:
    app_id = settings.service_name
    client = _inngest_clients.get(app_id)
    if client is None:
        client = inngest.Inngest(app_id=app_id)
        _inngest_clients[app_id] = client
    return client


def get_inngest_client(app_id: str) -> inngest.Inngest | None:
    return _inngest_clients.get(app_id)


async def start_inngest_connect_worker_from_settings(
    settings: Any,
    *,
    definitions_module: str,
    max_concurrency: int | None = None,
) -> Any:
    app_id = settings.service_name
    worker = _workers.get(app_id)
    if worker is not None:
        return worker

    importlib.import_module(definitions_module)
    client = initialize_inngest_client_from_settings(settings)

    def _get_job_service_client():
        provider = get_job_service_provider(settings.service_name)
        if provider is None:
            raise RuntimeError(f"Job service provider no inicializado para {settings.service_name}")
        return provider.client

    functions = build_inngest_functions(
        client=client,
        get_job_service_client=_get_job_service_client,
    )
    from inngest.experimental.connect import connect

    worker = connect(
        [(client, functions)],
        instance_id=getattr(settings, "service_name", app_id),
        max_concurrency=max_concurrency,
    )
    _workers[app_id] = worker
    import asyncio
    asyncio.create_task(worker.start())
    return worker


async def stop_inngest_connect_worker(app_id: str) -> None:
    worker = _workers.pop(app_id, None)
    if worker is None:
        return
    await worker.close(wait=True)


async def start_job_runtime_from_settings(
    settings: Any,
    *,
    definitions_module: str,
) -> Any | None:
    if getattr(settings, "job_runtime_engine", "inngest") == "inngest":
        return await start_inngest_connect_worker_from_settings(
            settings,
            definitions_module=definitions_module,
        )
    return None


async def stop_job_runtime_from_settings(settings: Any) -> None:
    if getattr(settings, "job_runtime_engine", "inngest") == "inngest":
        await stop_inngest_connect_worker(settings.service_name)
