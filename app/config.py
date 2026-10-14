from typing import Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseAppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    log_level: str = "INFO"


class ApiSettings(BaseAppSettings):
    api_key: SecretStr = Field(min_length=16)


class RelaySettings(BaseAppSettings):
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_poll_interval: float = Field(default=0.5, gt=0)
    outbox_error_backoff: float = Field(default=5.0, gt=0)


class ConsumerSettings(BaseAppSettings):
    consumer_prefetch: int = Field(default=10, ge=1)

    # attempt N failed -> wait retry_base_delay * 2 ** (N - 1) before the next one
    max_attempts: int = Field(default=3, ge=1, le=10)
    retry_base_delay: float = Field(default=2.0, gt=0)

    gateway_min_delay: float = Field(default=2.0, ge=0)
    gateway_max_delay: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    webhook_timeout: float = Field(default=10.0, gt=0)

    @model_validator(mode="after")
    def _check_gateway_delays(self) -> Self:
        if self.gateway_min_delay > self.gateway_max_delay:
            raise ValueError("gateway_min_delay must not exceed gateway_max_delay")
        return self
