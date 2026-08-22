from __future__ import annotations

import asyncio
import importlib
import logging
from typing import Any, Callable

import inngest

from job_service_sdk.jobs import build_inngest_functions
from job_service_sdk.registration import get_job_service_provider

logger = logging.getLogger(__name__)

RECONNECT_DELAY_SECONDS = 5.0

_inngest_clients: dict[str, inngest.Inngest] = {}
_workers: dict[str, Any] = {}
_supervisors: dict[str, asyncio.Task] = {}


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

    def build_worker() -> Any:
        return connect(
            [(client, functions)],
            instance_id=getattr(settings, "service_name", app_id),
            max_concurrency=max_concurrency,
        )

    worker = build_worker()
    _workers[app_id] = worker
    _supervisors[app_id] = asyncio.create_task(_keep_connected(app_id, worker, build_worker))
    return worker


async def _keep_connected(app_id: str, worker: Any, build_worker: Callable[[], Any]) -> None:
    """Mantiene viva la conexión con Inngest, reconectando si se cae.

    `connect` abre una conexión saliente que puede morir sin avisar. Sin esto,
    Inngest conserva las funciones registradas y no queda quien las ejecute:
    los jobs se quedan iniciados para siempre y nadie se entera.
    """
    while True:
        try:
            await worker.start()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Inngest worker for %s failed, reconnecting", app_id)
        else:
            logger.warning("Inngest worker for %s stopped, reconnecting", app_id)

        _workers.pop(app_id, None)
        await asyncio.sleep(RECONNECT_DELAY_SECONDS)
        worker = build_worker()
        _workers[app_id] = worker


def is_worker_connected(app_id: str) -> bool:
    """Si hay un supervisor vivo para esa app, o sea si los jobs se pueden ejecutar."""
    supervisor = _supervisors.get(app_id)
    return supervisor is not None and not supervisor.done()


async def stop_inngest_connect_worker(app_id: str) -> None:
    supervisor = _supervisors.pop(app_id, None)
    if supervisor is not None and not supervisor.done():
        supervisor.cancel()
        try:
            await supervisor
        except asyncio.CancelledError:
            pass

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
