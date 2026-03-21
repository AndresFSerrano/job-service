from enum import Enum


class JobExecutionStatus(str, Enum):
    REGISTERED = "registered"
    QUEUED = "queued"
    RUNNING = "running"
    WAITING_CALLBACK = "waiting_callback"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
