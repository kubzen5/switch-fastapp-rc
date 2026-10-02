from functools import lru_cache

from pydantic import Field, SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_host: str = "postgres"
    postgres_port: int = Field(default=5432, ge=1, le=65535)
    postgres_db: str = "switch"
    postgres_user: str = "switch"
    postgres_password: SecretStr = Field(min_length=1)
    kafka_bootstrap_servers: str = "redpanda:9092"
    kafka_topic: str = "source.orders.v1"
    kafka_delivery_timeout_ms: int = Field(default=10000, ge=1000, le=300000)
    kafka_publish_attempts: int = Field(default=4, ge=1, le=10)
    kafka_retry_base_seconds: float = Field(default=0.5, gt=0, le=60)
    kafka_retry_max_seconds: float = Field(default=5, gt=0, le=60)
    kafka_shutdown_timeout_seconds: float = Field(default=5, gt=0, le=60)
    kafka_group_id: str = "orders-materializer-v1"
    consumer_exclusive: bool = False
    log_level: str = "INFO"
    dependency_timeout_seconds: int = Field(default=60, gt=0)
    source_name: str = "snowflake.orders_source"
    sync_batch_size: int = Field(default=500, gt=0, le=50000)
    sync_interval_seconds: int = Field(default=30, gt=0)
    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_password: SecretStr = SecretStr("")
    snowflake_warehouse: str = ""
    snowflake_database: str = "SWITCH_DEMO"
    snowflake_schema: str = "PUBLIC"
    snowflake_table: str = "ORDERS_SOURCE"
    snowflake_role: str = ""


@lru_cache
def get_settings() -> Settings:
    try:
        return Settings()
    except ValidationError:
        # BaseSettings errors may include the entire unvalidated environment input.
        raise RuntimeError("Invalid environment configuration; verify required variables and their types") from None
