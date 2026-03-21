from persistence_kit.repository_factory import register_entity

from app.domain.entities.job_client import JobClient
from app.domain.entities.job_definition import JobDefinition
from app.domain.entities.job_execution import JobExecution
from app.domain.entities.job_event import JobEvent


def register_defaults() -> None:
    register_entity(
        "job_client",
        {
            "entity": JobClient,
            "collection": "job_clients",
            "unique": {"client_key": "client_key"},
        },
    )

    register_entity(
        "job_definition",
        {
            "entity": JobDefinition,
            "collection": "job_definitions",
            "unique": {"id": "id", "job_key": "job_key"},
        },
    )

    register_entity(
        "job_execution",
        {
            "entity": JobExecution,
            "collection": "job_executions",
            "unique": {"id": "id"},
        },
    )

    register_entity(
        "job_event",
        {
            "entity": JobEvent,
            "collection": "job_events",
            "unique": {"id": "id"},
        },
    )
