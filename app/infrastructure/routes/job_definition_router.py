import json
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from persistence_kit import Repository
from persistence_kit.repository_factory import provide_repo

from app.application.dto.job_definition_dto import JobDefinitionCreate, JobDefinitionOut
from app.application.use_cases.job_definition_use_cases import (
    list_job_definitions as list_job_definitions_use_case,
    register_job_definition as register_job_definition_use_case,
)
from app.domain.entities.job_definition import JobDefinition


JobDefinitionRepoDep = Annotated[Repository[JobDefinition, UUID], Depends(provide_repo("job_definition"))]

router = APIRouter(prefix="/job-definitions", tags=["Job Definitions"])


@router.post(
    "",
    response_model=JobDefinitionOut,
    status_code=status.HTTP_201_CREATED,
    summary="Registrar definición de job",
)
async def register_job_definition(
    payload: JobDefinitionCreate,
    repo: JobDefinitionRepoDep,
):
    try:
        definition = await register_job_definition_use_case(repo, payload)
        return _serialize_job_definition(definition)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))


@router.get(
    "",
    response_model=list[JobDefinitionOut],
    status_code=status.HTTP_200_OK,
    summary="Listar definiciones registradas",
)
async def list_job_definitions(
    repo: JobDefinitionRepoDep,
    client_key: str | None = Query(default=None),
):
    definitions = await list_job_definitions_use_case(repo, client_key=client_key)
    return [_serialize_job_definition(definition) for definition in definitions]


def _serialize_job_definition(definition: JobDefinition) -> dict[str, object]:
    return {
        "id": definition.id,
        "client_key": definition.client_key,
        "job_key": definition.job_key,
        "display_name": definition.display_name,
        "version": definition.version,
        "description": definition.description,
        "payload_schema": _load_json(definition.payload_schema),
        "result_schema": _load_json(definition.result_schema),
        "visibility_scope": definition.visibility_scope,
        "retry_policy": _load_json(definition.retry_policy),
        "execution_engine": definition.execution_engine,
        "execution_ref": definition.execution_ref,
        "active": definition.active,
        "created_at": definition.created_at,
    }


def _load_json(value: str | None) -> object:
    if value is None:
        return None
    return json.loads(value)
