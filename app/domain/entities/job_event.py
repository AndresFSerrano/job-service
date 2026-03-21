from dataclasses import dataclass, field
from uuid import UUID, uuid4

from app.core.time import now_bogota_iso

@dataclass(slots=True)
class JobEvent:
    job_execution_id: UUID
    event_type: str
    message: str
    data: str | None = None
    level: str = "info"
    created_at: str = field(default_factory=now_bogota_iso)
    id: UUID = field(default_factory=uuid4)
