from enum import Enum
from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import SettingsConfigDict
from persistence_kit import RepoSettings


class AuthProvider(str, Enum):
    MEMORY = "memory"
    COGNITO = "cognito"


class DeploymentStage(str, Enum):
    LOCAL = "local"
    DEV = "dev"
    PRODUCTION = "production"


LOCAL_DEFAULT_JOB_SERVICE_API_KEY = "local-job-service-dev-key"


class Settings(RepoSettings):
    stage: DeploymentStage = DeploymentStage.LOCAL
    auth_enabled: bool = False
    auth_provider: AuthProvider = AuthProvider.MEMORY
    memory_jwt_secret: str | None = None
    memory_jwt_issuer: str = "memory-sandbox"
    cognito_region: str = "us-east-1"
    cognito_user_pool_id: str | None = None
    cognito_app_client_id: str | None = None
    service_name: str = "job-service"
    service_version: str = "0.1.0"
    cors_origins: list[str] | None = None
    observability_enabled: bool = False
    log_level: str = "INFO"
    store_api_url: str = "http://api:8000"
    inngest_dev: str | None = None
    inngest_base_url: str | None = None
    inngest_public_url: str | None = None
    job_dispatch_stale_timeout_seconds: int = 300
    job_dispatch_reconcile_interval_seconds: int = 30
    api_path_prefix: str = ""
    job_service_api_key: str = LOCAL_DEFAULT_JOB_SERVICE_API_KEY

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def is_local_stage(self) -> bool:
        return self.stage == DeploymentStage.LOCAL

    @property
    def docs_enabled(self) -> bool:
        return self.is_local_stage

    @property
    def resolved_cors_origins(self) -> list[str]:
        if self.cors_origins:
            return self.cors_origins
        return ["*"] if self.is_local_stage else []

    @property
    def uses_local_default_job_service_api_key(self) -> bool:
        return self.is_local_stage and self.job_service_api_key == LOCAL_DEFAULT_JOB_SERVICE_API_KEY

    @model_validator(mode="after")
    def validate_security_requirements(self) -> "Settings":
        if self.auth_enabled and self.auth_provider == AuthProvider.MEMORY and not self.memory_jwt_secret:
            raise ValueError("MEMORY_JWT_SECRET is required when AUTH_PROVIDER=memory and AUTH_ENABLED=true.")

        if self.is_local_stage:
            return self

        if not self.auth_enabled:
            raise ValueError("AUTH_ENABLED must be true outside local stage.")

        if self.auth_provider != AuthProvider.COGNITO:
            raise ValueError("AUTH_PROVIDER must be 'cognito' outside local stage.")

        if not self.cognito_user_pool_id:
            raise ValueError("COGNITO_USER_POOL_ID is required outside local stage.")

        if not self.cognito_app_client_id:
            raise ValueError("COGNITO_APP_CLIENT_ID is required outside local stage.")

        if self.cors_origins and "*" in self.cors_origins:
            raise ValueError("CORS_ORIGINS cannot contain '*' outside local stage.")

        if self.job_service_api_key == LOCAL_DEFAULT_JOB_SERVICE_API_KEY:
            raise ValueError("JOB_SERVICE_API_KEY must be explicitly configured outside local stage.")

        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
