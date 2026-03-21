import json
from typing import Sequence
from uuid import UUID

from persistence_kit import Repository

from app.application.dto.job_definition_dto import JobDefinitionCreate
from app.domain.entities.job_definition import JobDefinition


async def register_job_definition(
    repo: Repository[JobDefinition, UUID],
    payload: JobDefinitionCreate,
) -> JobDefinition:
    normalized_payload = _normalize_job_definition_payload(payload)
    definitions = await repo.list_by_fields({"job_key": payload.job_key})
    for definition in definitions:
        if definition.client_key == payload.client_key and definition.version == payload.version:
            for key, value in normalized_payload.items():
                setattr(definition, key, value)
            await repo.update(definition)
            return definition
        raise ValueError(f"job_key '{payload.job_key}' is already registered")

    entity = JobDefinition(**normalized_payload)
    await repo.add(entity)
    return entity


async def list_job_definitions(
    repo: Repository[JobDefinition, UUID],
    *,
    client_key: str | None = None,
) -> Sequence[JobDefinition]:
    if client_key is not None:
        return await repo.list_by_fields({"client_key": client_key}, limit=100)
    return await repo.list(limit=100)


def _normalize_job_definition_payload(payload: JobDefinitionCreate) -> dict[str, object]:
    values = payload.model_dump()
    for key in ("payload_schema", "result_schema", "retry_policy"):
        value = values.get(key)
        if value is not None and not isinstance(value, str):
            values[key] = json.dumps(value, sort_keys=True)
    return values
