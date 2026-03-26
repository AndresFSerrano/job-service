from fastapi import APIRouter, status

router = APIRouter(
    prefix="/health",
    tags=["Health"],
)


@router.get(
    "",
    status_code=status.HTTP_200_OK,
    summary="Health check",
    description="Endpoint de salud para readiness/liveness.",
)
async def health() -> dict[str, str]:
    return {"status": "ok"}
