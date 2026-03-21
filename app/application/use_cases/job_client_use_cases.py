import json
from typing import Sequence
from uuid import UUID

from persistence_kit import Repository

from app.application.dto.job_client_dto import JobClientCreate, ServiceRegistrationCreate
from app.application.dto.job_definition_dto import JobDefinitionCreate
from app.application.use_cases.job_definition_use_cases import (
    register_job_definition as register_job_definition_use_case,
)
from app.domain.entities.job_client import JobClient
from app.domain.entities.job_definition import JobDefinition


async def register_job_client(
    repo: Repository[JobClient, UUID],
    payload: JobClientCreate,
) -> JobClient:
    normalized_payload = _normalize_job_client_payload(payload)
    entity = await _find_job_client_by_key(repo, payload.client_key)
    if entity is None:
        entity = JobClient(**normalized_payload)
        await repo.add(entity)
        return entity

    for key, value in normalized_payload.items():
        setattr(entity, key, value)
    await repo.update(entity)
    return entity


async def list_job_clients(repo: Repository[JobClient, UUID]) -> Sequence[JobClient]:
    return await repo.list(limit=100)


async def register_service_manifest(
    client_repo: Repository[JobClient, UUID],
    definition_repo: Repository[JobDefinition, UUID],
    payload: ServiceRegistrationCreate,
) -> tuple[JobClient, list[JobDefinition]]:
    client_payload = JobClientCreate(**payload.model_dump(exclude={"job_definitions"}))
    client = await register_job_client(client_repo, client_payload)

    registered_definitions: list[JobDefinition] = []
    for definition_payload in payload.job_definitions:
        definition_data = definition_payload.model_dump()
        definition_data["client_key"] = client.client_key
        registered_definitions.append(
            await register_job_definition_use_case(definition_repo, JobDefinitionCreate(**definition_data))
        )

    return client, registered_definitions


async def _find_job_client_by_key(
    repo: Repository[JobClient, UUID],
    client_key: str,
) -> JobClient | None:
    clients = await repo.list(limit=1000)
    for client in clients:
        if client.client_key == client_key:
            return client
    return None


def _normalize_job_client_payload(payload: JobClientCreate) -> dict[str, object]:
    values = payload.model_dump()
    metadata = values.get("metadata")
    if metadata is not None and not isinstance(metadata, str):
        values["metadata"] = json.dumps(metadata, sort_keys=True)
    return values
