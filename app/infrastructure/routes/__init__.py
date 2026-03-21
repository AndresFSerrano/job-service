from fastapi import APIRouter

from app.infrastructure.routes.job_client_router import router as client_router
from app.infrastructure.routes.job_definition_router import router as definition_router
from app.infrastructure.routes.job_event_router import router as event_router
from app.infrastructure.routes.job_execution_router import router as execution_router

router = APIRouter(prefix="/api/v1")
router.include_router(client_router)
router.include_router(definition_router)
router.include_router(execution_router)
router.include_router(event_router)
