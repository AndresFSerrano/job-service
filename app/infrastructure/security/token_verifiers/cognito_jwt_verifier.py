from collections.abc import Iterable
from typing import Any

from fastapi import HTTPException, status


class CognitoJwtVerifier:
    def __init__(
        self,
        *,
        region: str,
        user_pool_id: str,
        client_id: str | None = None,
        accepted_token_use: Iterable[str] = ("access", "id"),
    ) -> None:
        from jwt import PyJWKClient

        self._issuer = f"https://cognito-idp.{region}.amazonaws.com/{user_pool_id}"
        self._jwks_client = PyJWKClient(f"{self._issuer}/.well-known/jwks.json")
        self._client_id = client_id
        self._accepted_token_use = set(accepted_token_use)

    def verify(self, token: str) -> dict[str, Any]:
        import jwt

        try:
            signing_key = self._jwks_client.get_signing_key_from_jwt(token)
            payload = jwt.decode(
                token,
                key=signing_key.key,
                algorithms=["RS256"],
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
            if self._client_id:
                if token_use == "id" and payload.get("aud") != self._client_id:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="aud invalido.",
                    )
                if token_use == "access" and payload.get("client_id") != self._client_id:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="client_id invalido.",
                    )
            return payload
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token invalido o expirado.",
            ) from exc
