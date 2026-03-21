from dataclasses import dataclass, field
from uuid import UUID, uuid4

from app.core.time import now_bogota_iso

@dataclass(slots=True)
class JobClient:
    client_key: str
    display_name: str
    base_url: str
    description: str | None = None
    status: str = "active"
    callback_auth_mode: str | None = None
    healthcheck_url: str | None = None
    metadata: str | None = None
    created_at: str = field(default_factory=now_bogota_iso)
    id: UUID = field(default_factory=uuid4)
