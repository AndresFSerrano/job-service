from dataclasses import dataclass, field
from uuid import UUID, uuid4

from app.core.time import now_bogota_iso

@dataclass(slots=True)
class JobDefinition:
    client_key: str
    job_key: str
    display_name: str
    version: int = 1
    description: str | None = None
    payload_schema: str | None = None
    result_schema: str | None = None
    visibility_scope: str | None = None
    retry_policy: str | None = None
    execution_engine: str = "local"
    execution_ref: str | None = None
    active: bool = True
    allow_concurrent: bool = True
    created_at: str = field(default_factory=now_bogota_iso)
    id: UUID = field(default_factory=uuid4)
