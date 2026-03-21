# job-service

SDK cliente y runtime para integrarse con un `job_service` remoto.

Este paquete permite:

- registrar un servicio consumidor y su manifest de jobs
- crear ejecuciones remotas
- reportar progreso, eventos, resultados y errores
- construir flows reutilizables
- ejecutar workers con Inngest Connect o con un worker local simple

## Instalación

```bash
pip install job-service
```

## API pública principal

```python
from job_service import (
    JobClientConfig,
    JobDefinitionConfig,
    JobServiceClient,
    JobServiceProvider,
    build_service_client_config,
    bootstrap_job_service_provider,
    initialize_job_service_provider_from_settings,
    job_flow,
    build_job_definition_configs,
    start_job_runtime_from_settings,
    stop_job_runtime_from_settings,
    handler,
    JobWorker,
)
```

## Registrar un servicio y sus jobs

```python
from job_service import JobClientConfig, JobDefinitionConfig, JobServiceClient

client = JobServiceClient("http://localhost:8001")

client.register_service(
    JobClientConfig(
        client_key="sample-backend",
        display_name="Sample Backend",
        base_url="http://sample-backend:8000",
        healthcheck_url="http://sample-backend:8000/api/v1/health",
    ),
    [
        JobDefinitionConfig(
            client_key="sample-backend",
            job_key="sample_job",
            display_name="Sample Job",
        )
    ],
)
```

## Crear una ejecución

```python
from job_service import JobServiceClient

client = JobServiceClient("http://localhost:8001")

execution = client.create_execution(
    "sample_job",
    job_input={"sample_key": "sample_value"},
    requested_by_id="sample.user",
    requested_by_display="sample.user@example.com",
)
```

## Reportar progreso y completar

```python
from uuid import UUID

from job_service import JobServiceClient

client = JobServiceClient("http://localhost:8001")
job_id = UUID("00000000-0000-0000-0000-000000000001")

client.report_progress(
    job_id,
    progress_current=1,
    progress_total=3,
    progress_label="processing",
)

client.complete_execution(
    job_id,
    result={"processed_items": 4},
    result_summary="Job completado",
)
```

## Flow declarativo

```python
from job_service import JobFlow, job_flow


@job_flow(
    job_key="sample_job",
    display_name="Sample Job",
    description="Ejemplo de flow declarativo",
)
def build_sample_flow() -> JobFlow:
    flow = JobFlow()
    return flow
```

## Worker local

```python
from job_service import JobServiceClient, JobWorker, handler


@handler("sample_job")
async def sample_handler(job_id, job_input):
    return {"processed_items": 1}


client = JobServiceClient("http://localhost:8001")
worker = JobWorker(client)
```

## Inngest Connect

El paquete incluye helpers para levantar el runtime configurado a partir de `settings` y definiciones ya registradas.

```python
from job_service import (
    initialize_job_service_provider_from_settings,
    start_job_runtime_from_settings,
    stop_job_runtime_from_settings,
)

initialize_job_service_provider_from_settings(
    settings,
    definitions_module="sample.jobs_config",
)

await start_job_runtime_from_settings(
    settings,
    definitions_module="sample.jobs_config",
)

await stop_job_runtime_from_settings(settings)
```

Hoy el runtime soportado es `inngest`, pero la app consumidora ya no necesita decidir eso directamente.
