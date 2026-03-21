from typing import Annotated, Any

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.core.config import Settings, get_settings
from app.domain.security.authenticated_user import AuthenticatedUser
from app.domain.security.roles import Role
from app.infrastructure.security.factory import get_token_verifier

bearer_scheme = HTTPBearer(auto_error=False)


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


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Security(bearer_scheme)],
    request: Request,
    settings: Settings = Depends(get_settings),
) -> AuthenticatedUser:
    if not settings.auth_enabled:
        user = _auth_disabled_fallback_user()
        request.state.auth_user = user
        return user

    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No tiene permisos para acceder a este recurso.",
        )

    payload = get_token_verifier(settings).verify(credentials.credentials)
    username = _resolve_username_from_claims(payload) or payload.get("sub", "")
    user = AuthenticatedUser.from_values(
        subject=payload.get("sub", ""),
        username=username,
        email=payload.get("email"),
        roles=_resolve_roles_from_claims(payload),
    )
    request.state.auth_user = user
    return user


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
