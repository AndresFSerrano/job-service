from __future__ import annotations

import asyncio
import importlib
import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable

import httpx
import inngest

from job_service_sdk.jobs import build_inngest_functions
from job_service_sdk.registration import get_job_service_provider

logger = logging.getLogger(__name__)

RECONNECT_DELAY_SECONDS = 5.0
HEALTH_CHECK_INTERVAL_SECONDS = 15.0
HEALTH_CHECK_FAILURES_BEFORE_RECONNECT = 4
HEALTH_CHECK_TIMEOUT_SECONDS = 5.0
STALE_HEARTBEAT_SECONDS = 60.0
WORKER_CLOSE_TIMEOUT_SECONDS = 15.0
LIVE_CONNECTION_STATUSES = frozenset({"CONNECTED", "READY"})
WORKER_CONNECTION_QUERY = (
    "query WorkerConnection($id: ULID!) { workerConnection(connectionId: $id) { status lastHeartbeatAt } }"
)

_inngest_clients: dict[str, inngest.Inngest] = {}
_workers: dict[str, Any] = {}
_supervisors: dict[str, asyncio.Task] = {}
_unconfirmed_workers: set[str] = set()


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
    from inngest.connect import connect

    def build_worker() -> Any:
        return connect(
            [(client, functions)],
            instance_id=getattr(settings, "service_name", app_id),
            max_worker_concurrency=max_concurrency,
        )

    worker = build_worker()
    _workers[app_id] = worker
    _supervisors[app_id] = asyncio.create_task(_keep_connected(app_id, worker, build_worker, client))
    return worker


async def _keep_connected(
    app_id: str,
    worker: Any,
    build_worker: Callable[[], Any],
    client: inngest.Inngest | None = None,
) -> None:
    """Mantiene viva la conexión con Inngest, reconectando si se cae.

    `connect` abre una conexión saliente que puede morir sin avisar. Sin esto,
    Inngest conserva las funciones registradas y no queda quien las ejecute:
    los jobs se quedan iniciados para siempre y nadie se entera. Además de
    reconectar cuando `start()` termina, una vigilancia descarta el worker que
    sigue vivo para inngest pero ya no está conectado de verdad.
    """
    while True:
        try:
            await _run_until_disconnected(app_id, worker, client)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Inngest worker for %s failed, reconnecting", app_id)

        _workers.pop(app_id, None)
        await asyncio.sleep(RECONNECT_DELAY_SECONDS)
        worker = build_worker()
        _workers[app_id] = worker


async def _run_until_disconnected(app_id: str, worker: Any, client: inngest.Inngest | None) -> None:
    """Corre el worker hasta que inngest lo suelta o hasta que la vigilancia lo da por perdido.

    `start()` solo vuelve cuando inngest nota la caída. Si la conexión queda muerta
    sin que lo note, `start()` se queda esperando para siempre; por eso corre junto
    a `_watch_connection` y gana el primero que termine.
    """
    _unconfirmed_workers.discard(app_id)
    run = asyncio.create_task(worker.start())
    async with httpx.AsyncHTTPTransport() as transport:
        watchdog = asyncio.create_task(_watch_connection(app_id, worker, client, transport))
        try:
            await asyncio.wait((run, watchdog), return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            run.cancel()
            watchdog.cancel()
            raise
        watchdog.cancel()
        await asyncio.gather(watchdog, return_exceptions=True)

    if run.done():
        await run
        logger.warning("Inngest worker for %s stopped, reconnecting", app_id)
        return

    logger.warning("Inngest worker for %s is not connected (%s), reconnecting", app_id, watchdog.result())
    await _retire_worker(app_id, worker, run)


async def _watch_connection(
    app_id: str,
    worker: Any,
    client: inngest.Inngest | None,
    transport: httpx.AsyncBaseTransport,
) -> str:
    """Revisa cada cierto tiempo si el worker sigue conectado y vuelve cuando deja de estarlo.

    Solo actúa tras varias revisiones fallidas seguidas, para no tumbar una conexión
    que se está recuperando sola. Una revisión sin respuesta del servidor no cuenta
    ni a favor ni en contra. Devuelve el motivo de la última revisión fallida.
    """
    failures = 0
    reason = ""
    while failures < HEALTH_CHECK_FAILURES_BEFORE_RECONNECT:
        await asyncio.sleep(HEALTH_CHECK_INTERVAL_SECONDS)
        try:
            connected, detail = await _check_worker_connection(worker, client, transport)
        except Exception:
            logger.debug("Inngest worker check for %s failed", app_id, exc_info=True)
            continue
        if connected is None:
            logger.debug("Inngest worker check for %s inconclusive: %s", app_id, detail)
            continue
        if connected:
            failures = 0
            _unconfirmed_workers.discard(app_id)
            continue
        failures += 1
        reason = detail
        _unconfirmed_workers.add(app_id)
        logger.debug(
            "Inngest worker for %s looks disconnected (%s), check %s of %s",
            app_id,
            detail,
            failures,
            HEALTH_CHECK_FAILURES_BEFORE_RECONNECT,
        )
    return reason


async def _check_worker_connection(
    worker: Any,
    client: inngest.Inngest | None,
    transport: httpx.AsyncBaseTransport,
) -> tuple[bool | None, str]:
    """Dice si el worker está conectado: `True`, `False` o `None` cuando no se puede saber.

    Primero mira el estado que reporta inngest. Si dice que está activa y la app
    habla con un Dev Server, le pregunta al servidor por esa conexión, porque una
    conexión muerta puede seguir activa para inngest.
    """
    from inngest.connect import ConnectionState

    state = worker.get_state()
    if state != ConnectionState.ACTIVE:
        return False, f"connection state is {state.value}"
    if client is None or client.is_production:
        return True, "connection state is ACTIVE"
    return await _probe_worker_connection(transport, client.api_origin, worker.get_connection_id())


async def _probe_worker_connection(
    transport: httpx.AsyncBaseTransport,
    api_origin: str,
    connection_id: str,
) -> tuple[bool | None, str]:
    """Le pregunta al Dev Server si conoce la conexión y si le siguen llegando sus latidos.

    Devuelve `None` cuando no hay una respuesta útil (servidor caído, otra versión de
    la API), para no confundir un Inngest que se está reiniciando con una conexión muerta.
    La consulta va directo al transporte para no dejar una línea de log de httpx en cada revisión.
    """
    request = httpx.Request(
        "POST",
        f"{api_origin.rstrip('/')}/v0/gql",
        json={"query": WORKER_CONNECTION_QUERY, "variables": {"id": connection_id}},
        extensions={"timeout": httpx.Timeout(HEALTH_CHECK_TIMEOUT_SECONDS).as_dict()},
    )
    try:
        response = await transport.handle_async_request(request)
        await response.aread()
    except httpx.HTTPError as exc:
        return None, f"Inngest API unreachable: {exc.__class__.__name__}"
    if response.status_code != 200:
        return None, f"Inngest API answered {response.status_code}"
    try:
        payload = response.json()
    except ValueError:
        return None, "Inngest API answered something that is not JSON"
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict) or "workerConnection" not in data:
        return None, "Inngest API does not expose workerConnection"

    connection = data["workerConnection"]
    if not isinstance(connection, dict):
        return False, f"the Inngest server does not know connection {connection_id}"
    status = connection.get("status")
    if status not in LIVE_CONNECTION_STATUSES:
        return False, f"the Inngest server reports connection {connection_id} as {status}"
    heartbeat_age = _seconds_since(connection.get("lastHeartbeatAt"), response.headers.get("date"))
    if heartbeat_age is not None and heartbeat_age > STALE_HEARTBEAT_SECONDS:
        return False, f"the Inngest server got no heartbeat from connection {connection_id} for {heartbeat_age:.0f}s"
    return True, f"the Inngest server reports connection {connection_id} as {status}"


def _seconds_since(timestamp: Any, server_date: str | None) -> float | None:
    """Segundos entre `timestamp` y la hora del servidor, sin depender del reloj local si hay `Date`."""
    if not isinstance(timestamp, str) or not timestamp:
        return None
    try:
        moment = datetime.fromisoformat(timestamp)
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return (_server_now(server_date) - moment).total_seconds()


def _server_now(server_date: str | None) -> datetime:
    if server_date:
        try:
            return parsedate_to_datetime(server_date)
        except (TypeError, ValueError):
            pass
    return datetime.now(timezone.utc)


async def _retire_worker(app_id: str, worker: Any, run: asyncio.Task) -> None:
    """Cierra un worker que perdió la conexión sin esperarlo más de la cuenta.

    El cierre de inngest espera a que terminen las ejecuciones en curso; si no termina
    a tiempo, el worker viejo se deja cerrando por su cuenta y se arma uno nuevo.
    """
    try:
        await worker.close()
    except Exception:
        logger.exception("Inngest worker for %s could not be closed", app_id)
    done, _ = await asyncio.wait((run,), timeout=WORKER_CLOSE_TIMEOUT_SECONDS)
    if not done:
        logger.warning("Inngest worker for %s did not stop in time, leaving it to close in the background", app_id)
    run.add_done_callback(_log_retired_worker_result)


def _log_retired_worker_result(run: asyncio.Task) -> None:
    if run.cancelled():
        return
    exc = run.exception()
    if exc is not None:
        logger.debug("Retired Inngest worker ended with %r", exc)


def is_worker_connected(app_id: str) -> bool:
    """Si el worker de esa app está conectado de verdad, o sea si los jobs se pueden ejecutar.

    Pide un supervisor vivo, que inngest reporte la conexión como activa y que la
    última revisión contra el servidor no la haya dado por perdida.
    """
    supervisor = _supervisors.get(app_id)
    if supervisor is None or supervisor.done():
        return False
    worker = _workers.get(app_id)
    if worker is None or app_id in _unconfirmed_workers:
        return False
    from inngest.connect import ConnectionState

    return worker.get_state() == ConnectionState.ACTIVE


async def stop_inngest_connect_worker(app_id: str) -> None:
    supervisor = _supervisors.pop(app_id, None)
    if supervisor is not None and not supervisor.done():
        supervisor.cancel()
        try:
            await supervisor
        except asyncio.CancelledError:
            pass

    _unconfirmed_workers.discard(app_id)
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
