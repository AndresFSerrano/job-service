import logging

from fastapi import FastAPI
from persistence_kit.bootstrap.configuration import ConfigRegistry, set_config_package
from persistence_kit.bootstrap.seeders import SeederProvider
from persistence_kit.bootstrap.startup import run_startup_bootstrap
from persistence_kit.repository_factory import set_registry_initializer
from starlette.middleware.cors import CORSMiddleware

from app.core.config import get_settings
from app.infrastructure.exception_handlers import register_exception_handlers
from app.infrastructure.repository_factory.register_defaults import register_defaults
from app.infrastructure.routes import router as api_router
from app.infrastructure.routes.health_router import router as health_router
from app.workers.inngest_dispatcher import InngestJobDispatcher, set_dispatcher

logger = logging.getLogger(__name__)
dispatcher: InngestJobDispatcher | None = None

set_config_package("app.core")
set_registry_initializer(register_defaults)

def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="Job Service",
        version=settings.service_version,
        description="Orquesta el ciclo de vida de trabajos transversales para multiples proyectos.",
        docs_url="/docs" if settings.docs_enabled else None,
        redoc_url="/redoc" if settings.docs_enabled else None,
        openapi_url="/openapi.json" if settings.docs_enabled else None,
        swagger_ui_parameters={
            "docExpansion": "list",
            "defaultModelsExpandDepth": -1,
            "displayRequestDuration": True,
        },
    )

    app.include_router(api_router)
    app.include_router(health_router)
    register_exception_handlers(app)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.resolved_cors_origins,
        allow_credentials=False,
        allow_methods=["GET", "POST", "PATCH", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "Accept"],
    )

    @app.on_event("startup")
    async def _init_config() -> None:
        async def _run_bootstrap_components() -> None:
            ConfigRegistry.run_all()
            await SeederProvider().run_all()

        await run_startup_bootstrap(settings, logger, _run_bootstrap_components)
        global dispatcher
        dispatcher = InngestJobDispatcher(settings)
        set_dispatcher(dispatcher)
        await dispatcher.start()

    @app.on_event("shutdown")
    async def _stop_dispatcher() -> None:
        if dispatcher is not None:
            await dispatcher.stop()
        set_dispatcher(None)

    return app


app = create_app()
