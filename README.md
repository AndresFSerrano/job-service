# job-service-sdk

Repositorio del SDK `job-service-sdk`, publicado en [PyPI](https://pypi.org/project/job-service-sdk/). Las aplicaciones lo usan para registrar sus jobs en job-service, definir los flujos con `@job_flow`, crear ejecuciones y ejecutar los pasos con Inngest Connect.

- `client/`: el paquete (código en `client/src/job_service_sdk`, pruebas en `client/tests`). Su README tiene la API pública.
- `.github/workflows/publish-pypi.yml`: publica una versión cuando se crea un Release con tag `vX.Y.Z` sobre la punta de `develop`.
- `.github/workflows/publish-preview.yml`: publica versiones de prueba (`X.Y.Z.devN`) a mano, en TestPyPI o PyPI.

El servicio job-service vive en [Equipo-de-desarrollo-FCEN-UDEA/job-service](https://github.com/Equipo-de-desarrollo-FCEN-UDEA/job-service).

## Desarrollo

```bash
cd client
poetry install
poetry run pytest
```

## Publicar una versión

1. Subir `version` en `client/pyproject.toml` y fusionar en `develop`.
2. Crear un Release con tag `vX.Y.Z` sobre la punta de `develop`.
3. `publish-pypi.yml` construye `client/` y publica con Trusted Publishing, atado a este repositorio, a ese workflow y al environment `pypi`. Si cambia el nombre del repositorio, del workflow o del environment, PyPI deja de aceptar la publicación.
