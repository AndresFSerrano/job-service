from dataclasses import dataclass
from typing import Iterable

from app.domain.security.roles import Role


@dataclass(frozen=True)
class AuthenticatedUser:
    subject: str
    username: str
    email: str | None
    roles: tuple[Role, ...]

    @property
    def is_admin_general(self) -> bool:
        return Role.ADMIN_GENERAL in self.roles

    @classmethod
    def from_values(
        cls,
        *,
        subject: str,
        username: str | None = None,
        email: str | None,
        roles: Iterable[Role],
    ) -> "AuthenticatedUser":
        resolved_username = username.strip() if isinstance(username, str) and username.strip() else subject
        return cls(subject=subject, username=resolved_username, email=email, roles=tuple(roles))
