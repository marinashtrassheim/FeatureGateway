from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Основной Redis/KeyDB (JSON в значениях hash).
    KEYDB_DS_URL: str = "redis://localhost:6379/0"

    KEYDB_DS_SECOND_URL: str = "redis://localhost:6379/0"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    KEYDB_CONNECT_TIMEOUT: int = 5
    KEYDB_READ_TIMEOUT: int = 10
    KEYDB_MAX_CONNECTIONS: int = 50

    PERS_COLS_CACHE_TTL_SECONDS: int = 86_400

    FEATURE_RESPONSE_CACHE_URL: str = ""
    FEATURE_RESPONSE_CACHE_TTL_SECONDS: int = 1800


settings = Settings()
