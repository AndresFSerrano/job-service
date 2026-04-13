from functools import lru_cache

from fastapi import HTTPException, status

from app.core.config import AuthProvider, Settings
from persistence_kit.security.token_verifiers import CognitoJwtVerifier, MemoryJwtVerifier


@lru_cache
def _memory_token_verifier(secret: str, issuer: str) -> MemoryJwtVerifier:
    return MemoryJwtVerifier(secret=secret, issuer=issuer)


@lru_cache
def _token_verifier_cached(
    provider: str,
    cognito_region: str,
    cognito_user_pool_id: str,
    cognito_client_id: str | None,
    memory_jwt_secret: str,
    memory_jwt_issuer: str,
):
    if provider == AuthProvider.MEMORY.value:
        if not memory_jwt_secret:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuracion MEMORY_JWT_SECRET.",
            )
        return _memory_token_verifier(memory_jwt_secret, memory_jwt_issuer)
    if provider == AuthProvider.COGNITO.value:
        if not cognito_user_pool_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuracion COGNITO_USER_POOL_ID.",
            )
        return CognitoJwtVerifier(
            region=cognito_region,
            user_pool_id=cognito_user_pool_id,
            client_id=cognito_client_id,
        )
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Proveedor de autenticacion no soportado: '{provider}'.",
    )


def get_token_verifier(settings: Settings):
    return _token_verifier_cached(
        settings.auth_provider.value,
        settings.cognito_region,
        settings.cognito_user_pool_id or "",
        settings.cognito_app_client_id,
        settings.memory_jwt_secret or "",
        settings.memory_jwt_issuer,
    )
