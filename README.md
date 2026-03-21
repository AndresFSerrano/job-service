# job_service

Repositorio del servicio `job_service`.

Contenido principal:

- `app/`: servidor FastAPI y lógica del servicio
- `client/`: paquete publicable del SDK cliente
- `tests/`: pruebas del servidor y del SDK
- `infra/`: compose y soporte de despliegue local

## SDK cliente

El paquete publicable vive en:

- [client](C:/Users/andre/OneDrive/Escritorio/aux%20programación%20udea/job_service/client)

Su README específico está en:

- [client/README.md](C:/Users/andre/OneDrive/Escritorio/aux%20programación%20udea/job_service/client/README.md)

## Servidor local

El servidor arranca desde:

```bash
uvicorn app.main:app --reload
```
