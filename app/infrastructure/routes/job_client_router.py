import json
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from persistence_kit import Repository
from persistence_kit.repository_factory import provide_repo

from app.application.dto.job_client_dto import (
    JobClientCreate,
    JobClientOut,
    ServiceRegistrationCreate,
    ServiceRegistrationOut,
)
from app.application.use_cases.job_client_use_cases import (
    list_job_clients as list_job_clients_use_case,
    register_job_client as register_job_client_use_case,
    register_service_manifest as register_service_manifest_use_case,
)
from app.domain.entities.job_client import JobClient
from app.domain.entities.job_definition import JobDefinition


JobClientRepoDep = Annotated[Repository[JobClient, UUID], Depends(provide_repo("job_client"))]
JobDefinitionRepoDep = Annotated[
    Repository[JobDefinition, UUID],
    Depends(provide_repo("job_definition")),
]

router = APIRouter(prefix="/job-clients", tags=["Job Clients"])


@router.post(
    "",
    response_model=JobClientOut,
    status_code=status.HTTP_201_CREATED,
    summary="Registrar cliente",
)
async def register_job_client(
    payload: JobClientCreate,
    repo: JobClientRepoDep,
):
    client = await register_job_client_use_case(repo, payload)
    return _serialize_job_client(client)


@router.get(
    "",
    response_model=list[JobClientOut],
    status_code=status.HTTP_200_OK,
    summary="Listar clientes",
)
async def list_job_clients(
    repo: JobClientRepoDep,
):
    clients = await list_job_clients_use_case(repo)
    return [_serialize_job_client(client) for client in clients]


@router.post(
    "/service-registration",
    response_model=ServiceRegistrationOut,
    status_code=status.HTTP_201_CREATED,
    summary="Registrar servicio consumidor y sus jobs",
)
async def register_service_manifest_route(
    payload: ServiceRegistrationCreate,
    client_repo: JobClientRepoDep,
    definition_repo: JobDefinitionRepoDep,
):
    try:
        client, definitions = await register_service_manifest_use_case(client_repo, definition_repo, payload)
        return {
            "client": _serialize_job_client(client),
            "job_definitions": [_serialize_job_definition(definition) for definition in definitions],
        }
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))


def _serialize_job_client(client: JobClient) -> dict[str, object]:
    return {
        "id": client.id,
        "client_key": client.client_key,
        "display_name": client.display_name,
        "base_url": client.base_url,
        "description": client.description,
        "status": client.status,
        "callback_auth_mode": client.callback_auth_mode,
        "healthcheck_url": client.healthcheck_url,
        "metadata": json.loads(client.metadata) if client.metadata else None,
        "created_at": client.created_at,
    }


def _serialize_job_definition(definition: JobDefinition) -> dict[str, object]:
    return {
        "id": definition.id,
        "client_key": definition.client_key,
        "job_key": definition.job_key,
        "display_name": definition.display_name,
        "version": definition.version,
        "description": definition.description,
        "payload_schema": json.loads(definition.payload_schema) if definition.payload_schema else None,
        "result_schema": json.loads(definition.result_schema) if definition.result_schema else None,
        "visibility_scope": definition.visibility_scope,
        "retry_policy": json.loads(definition.retry_policy) if definition.retry_policy else None,
        "execution_engine": definition.execution_engine,
        "execution_ref": definition.execution_ref,
        "active": definition.active,
        "created_at": definition.created_at,
    }
