import logging

from fastapi import FastAPI, HTTPException, Request
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)

GENERIC_SERVER_ERROR_DETAIL = "Error interno del servidor."


async def handle_http_exception(
    request: Request,
    exc: HTTPException,
) -> JSONResponse:
    if exc.status_code < 500:
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=exc.headers,
        )

    logger.error(
        "http_exception_server_error",
        extra={
            "path": request.url.path,
            "method": request.method,
            "status_code": exc.status_code,
            "error_detail": exc.detail,
        },
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": GENERIC_SERVER_ERROR_DETAIL},
        headers=exc.headers,
    )


async def handle_unexpected_exception(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    logger.exception(
        "unhandled_exception",
        extra={
            "path": request.url.path,
            "method": request.method,
        },
    )
    return JSONResponse(
        status_code=500,
        content={"detail": GENERIC_SERVER_ERROR_DETAIL},
    )


def register_exception_handlers(api: FastAPI) -> None:
    api.add_exception_handler(HTTPException, handle_http_exception)
    api.add_exception_handler(Exception, handle_unexpected_exception)
