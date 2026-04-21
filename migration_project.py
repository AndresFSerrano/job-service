from __future__ import annotations

import os
from pathlib import Path

from db_migration_kit import MigrationProject, MigrationProjectSettings
from db_migration_kit.sources.persistence_kit_registry import PersistenceKitRegistrySchemaSource
from app.core.config import get_settings


def _read_env_value(name: str) -> str | None:
    env_value = os.environ.get(name)
    if env_value:
        return env_value
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return None
    for raw_line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() == name:
            return value.strip().strip("'").strip('"')
    return None


def _build_async_database_url() -> str:
    direct_url = _read_env_value("MIGRATION_DATABASE_URL") or _read_env_value("DATABASE_URL")
    if direct_url:
        return direct_url
    settings = get_settings()
    postgres_dsn = getattr(settings, "postgres_dsn", None)
    if postgres_dsn:
        return postgres_dsn
    postgres_host, postgres_port = _resolve_postgres_target(settings.postgres_host, settings.postgres_port)
    return (
        f"postgresql+asyncpg://{settings.postgres_user}:{settings.postgres_password}"
        f"@{postgres_host}:{postgres_port}/{settings.postgres_db}"
    )


def _build_sync_database_url() -> str | None:
    direct_url = _read_env_value("MIGRATION_SYNC_DATABASE_URL") or _read_env_value("SYNC_DATABASE_URL")
    if direct_url:
        return direct_url
    async_url = _build_async_database_url()
    if async_url.startswith("postgresql+asyncpg://"):
        return async_url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
    return async_url


def _resolve_postgres_target(host: str | None, port: int | None) -> tuple[str | None, int | None]:
    if host != "postgres":
        return host, port
    try:
        import socket
        socket.getaddrinfo(host, None)
        return host, port
    except OSError:
        host_port = _read_env_value("POSTGRES_PORT_HOST")
        resolved_port = int(host_port) if host_port else port
        return "localhost", resolved_port


class ProjectMigration(MigrationProject):
    def get_settings(self) -> MigrationProjectSettings:
        return MigrationProjectSettings(
            project_name="job-service",
            migrations_dir=Path(__file__).resolve().parent / "migrations",
            database_url=_build_async_database_url(),
            sync_database_url=_build_sync_database_url(),
            provider_name=os.environ.get("MIGRATION_PROVIDER", "sqlalchemy-postgres"),
            metadata_import_path=os.environ.get("MIGRATION_METADATA_IMPORT"),
        )

    def get_schema_source(self):
        return PersistenceKitRegistrySchemaSource(
            registry_initializer_import_path="app.infrastructure.repository_factory.register_defaults:register_defaults"
        )


project = ProjectMigration()
