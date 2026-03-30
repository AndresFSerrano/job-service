from dataclasses import dataclass
from hmac import compare_digest
from typing import Annotated, Any, TypeAlias

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.core.config import Settings, get_settings
from app.domain.security.authenticated_user import AuthenticatedUser
from app.domain.security.roles import Role
from app.infrastructure.security.factory import get_token_verifier

bearer_scheme = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class ServicePrincipal:
    subject: str = "job-service-internal"
    username: str = "job-service-internal"
    email: str | None = None


RequestPrincipal: TypeAlias = AuthenticatedUser | ServicePrincipal


def _resolve_roles_from_claims(payload: dict[str, Any]) -> tuple[Role, ...]:
    raw_roles = payload.get("cognito:groups") or payload.get("roles") or []
    if isinstance(raw_roles, str):
        raw_roles = [raw_roles]

    roles: list[Role] = []
    for item in raw_roles:
        value = str(item).strip().lower()
        try:
            roles.append(Role(value))
        except ValueError:
            continue
    return tuple(roles)


def _resolve_username_from_claims(payload: dict[str, Any]) -> str | None:
    for field in ("cognito:username", "username", "preferred_username"):
        value = payload.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _auth_disabled_fallback_user() -> AuthenticatedUser:
    return AuthenticatedUser.from_values(
        subject="local-dev",
        username="local-dev",
        email="local@udea.edu.co",
        roles=[Role.ADMIN_GENERAL],
    )


def _build_user_from_payload(payload: dict[str, Any]) -> AuthenticatedUser:
    username = _resolve_username_from_claims(payload) or payload.get("sub", "")
    return AuthenticatedUser.from_values(
        subject=payload.get("sub", ""),
        username=username,
        email=payload.get("email"),
        roles=_resolve_roles_from_claims(payload),
    )


def _store_request_principal(request: Request, principal: RequestPrincipal) -> None:
    request.state.auth_principal = principal
    request.state.auth_user = principal if isinstance(principal, AuthenticatedUser) else None


def _matches_service_api_key(token: str, settings: Settings) -> bool:
    return bool(settings.job_service_api_key) and compare_digest(token, settings.job_service_api_key)


def _unauthorized_exception() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No tiene permisos para acceder a este recurso.",
    )


async def get_request_principal(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Security(bearer_scheme)],
    request: Request,
    settings: Settings = Depends(get_settings),
) -> RequestPrincipal:
    if credentials is None:
        if settings.is_local_stage and not settings.auth_enabled and settings.uses_local_default_job_service_api_key:
            principal = _auth_disabled_fallback_user()
            _store_request_principal(request, principal)
            return principal
        raise _unauthorized_exception()

    token = credentials.credentials
    if _matches_service_api_key(token, settings):
        principal = ServicePrincipal()
        _store_request_principal(request, principal)
        return principal

    if not settings.auth_enabled:
        if settings.is_local_stage and settings.uses_local_default_job_service_api_key:
            principal = _auth_disabled_fallback_user()
            _store_request_principal(request, principal)
            return principal
        raise _unauthorized_exception()

    principal = _build_user_from_payload(get_token_verifier(settings).verify(token))
    _store_request_principal(request, principal)
    return principal


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Security(bearer_scheme)],
    request: Request,
    settings: Settings = Depends(get_settings),
) -> AuthenticatedUser:
    principal = await get_request_principal(credentials, request, settings)
    if isinstance(principal, ServicePrincipal):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Se requiere un token de usuario para acceder a este recurso.",
        )
    return principal


async def require_service_principal(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Security(bearer_scheme)],
    request: Request,
    settings: Settings = Depends(get_settings),
) -> ServicePrincipal:
    if settings.is_local_stage and not settings.auth_enabled and settings.uses_local_default_job_service_api_key:
        principal = ServicePrincipal()
        _store_request_principal(request, principal)
        return principal
    principal = await get_request_principal(credentials, request, settings)
    if not isinstance(principal, ServicePrincipal):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Se requiere una credencial de servicio para acceder a este recurso.",
        )
    return principal


async def require_admin_or_service_principal(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Security(bearer_scheme)],
    request: Request,
    settings: Settings = Depends(get_settings),
) -> RequestPrincipal:
    principal = await get_request_principal(credentials, request, settings)
    if isinstance(principal, ServicePrincipal) or principal.is_admin_general:
        return principal
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="No tiene permisos para acceder a este recurso.",
    )


def ensure_principal_can_access_execution(
    principal: RequestPrincipal,
    requested_by_id: str | None,
) -> None:
    if isinstance(principal, ServicePrincipal) or principal.is_admin_general:
        return
    if requested_by_id == principal.username:
        return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="No tiene permisos para consultar esta ejecución.",
    )


def ensure_user_can_access_manager_type(
    current_user: AuthenticatedUser,
    manager_type: str | None,
) -> None:
    if not manager_type:
        return
    normalized = manager_type.strip().upper()
    allowed_roles_by_manager = {
        "UCARA": {Role.ADMIN_GENERAL, Role.ADMIN_UCARA, Role.AUXILIAR_UCARA},
        "ALMACEN": {Role.ADMIN_GENERAL, Role.ADMIN_ALMACEN, Role.AUXILIAR_ALMACEN},
    }
    allowed_roles = allowed_roles_by_manager.get(normalized, set())
    if set(current_user.roles).intersection(allowed_roles):
        return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"No tienes permisos para operar sobre manager '{normalized}'.",
    )
