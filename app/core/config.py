from enum import Enum
from functools import lru_cache

from pydantic_settings import SettingsConfigDict
from persistence_kit import RepoSettings


class AuthProvider(str, Enum):
    MEMORY = "memory"
    COGNITO = "cognito"


class Settings(RepoSettings):
    auth_enabled: bool = False
    auth_provider: AuthProvider = AuthProvider.MEMORY
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

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
