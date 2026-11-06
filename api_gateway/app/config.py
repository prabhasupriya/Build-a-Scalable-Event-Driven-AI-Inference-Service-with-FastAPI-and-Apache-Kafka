"""Runtime configuration, read exclusively from environment variables."""
import os
from dataclasses import dataclass
from functools import lru_cache

from app.retry import RetryPolicy


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be a number, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    service_name: str = "api_gateway"

    fastapi_host: str = "0.0.0.0"
    fastapi_port: int = 8000

    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_requests: str = "inference_requests"
    kafka_topic_partitions: int = 3
    kafka_topic_replication_factor: int = 1

    db_host: str = "postgres"
    db_port: int = 5432
    db_user: str = "user"
    db_password: str = "password"
    db_name: str = "inference_db"

    log_level: str = "INFO"

    retry_max_attempts: int = 5
    retry_base_delay_seconds: float = 0.5
    retry_max_delay_seconds: float = 10.0

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            fastapi_host=_env_str("FASTAPI_HOST", cls.fastapi_host),
            fastapi_port=_env_int("FASTAPI_PORT", cls.fastapi_port),
            kafka_bootstrap_servers=_env_str("KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap_servers),
            kafka_topic_requests=_env_str("KAFKA_TOPIC_REQUESTS", cls.kafka_topic_requests),
            kafka_topic_partitions=_env_int("KAFKA_TOPIC_PARTITIONS", cls.kafka_topic_partitions),
            kafka_topic_replication_factor=_env_int(
                "KAFKA_TOPIC_REPLICATION_FACTOR", cls.kafka_topic_replication_factor
            ),
            db_host=_env_str("DB_HOST", cls.db_host),
            db_port=_env_int("DB_PORT", cls.db_port),
            db_user=_env_str("DB_USER", cls.db_user),
            db_password=_env_str("DB_PASSWORD", cls.db_password),
            db_name=_env_str("DB_NAME", cls.db_name),
            log_level=_env_str("LOG_LEVEL", cls.log_level),
            retry_max_attempts=_env_int("RETRY_MAX_ATTEMPTS", cls.retry_max_attempts),
            retry_base_delay_seconds=_env_float(
                "RETRY_BASE_DELAY_SECONDS", cls.retry_base_delay_seconds
            ),
            retry_max_delay_seconds=_env_float(
                "RETRY_MAX_DELAY_SECONDS", cls.retry_max_delay_seconds
            ),
        )

    @property
    def retry_policy(self) -> RetryPolicy:
        return RetryPolicy(
            max_attempts=self.retry_max_attempts,
            base_delay=self.retry_base_delay_seconds,
            max_delay=self.retry_max_delay_seconds,
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()
