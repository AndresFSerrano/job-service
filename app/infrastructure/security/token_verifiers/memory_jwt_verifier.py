from collections.abc import Iterable
from typing import Any

from fastapi import HTTPException, status


class MemoryJwtVerifier:
    def __init__(
        self,
        *,
        secret: str,
        issuer: str,
        accepted_token_use: Iterable[str] = ("access", "id"),
    ) -> None:
        self._secret = secret
        self._issuer = issuer
        self._accepted_token_use = set(accepted_token_use)

    def verify(self, token: str) -> dict[str, Any]:
        import jwt

        try:
            payload = jwt.decode(
                token,
                key=self._secret,
                algorithms=["HS256"],
                issuer=self._issuer,
                audience=None,
                options={"verify_aud": False},
            )
            token_use = payload.get("token_use")
            if token_use not in self._accepted_token_use:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="token_use invalido.",
                )
            return payload
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token invalido o expirado.",
            ) from exc
