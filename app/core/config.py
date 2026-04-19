from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # KeyDB: в common_rank_wrapper две сущности — db_conn_1 (pers_cols, pers_hub_city,
    # pers_user_city, pers_user_item, pers_item) и db_conn_2 (pers_offl). На проде URL могут
    # различаться; локально часто один инстанс — тогда оба URL одинаковые.
    # Репозиторий шлюза: использовать KEYDB_DS_URL для «первой» БД и KEYDB_DS_SECOND_URL для
    # офлайна; при смене топологии править только настройки и фабрику клиентов, не контракт API.
    KEYDB_DS_URL: str = "redis://localhost:6379/0"
    KEYDB_DS_SECOND_URL: str = "redis://localhost:6379/0"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True
    )
    # Варианты хранения
    STORAGE_VERSION: str = "v1"  # v1, v2, v3

    # URL для разных вариантов хранения (локально см. docker-compose.yml и .env)
    KEYDB_V1_URL: str = "redis://localhost:6379/0"
    KEYDB_V2_URL: str = "redis://localhost:6380/0"
    KEYDB_V3_URL: str = "redis://localhost:6381/0"



    # Таймауты и пр. (общие)
    KEYDB_CONNECT_TIMEOUT: int = 5
    KEYDB_READ_TIMEOUT: int = 10
    KEYDB_MAX_CONNECTIONS: int = 50

    # In-process TTL для pers_cols (секунды). Данные в KeyDB обновляются ~раз в сутки.
    PERS_COLS_CACHE_TTL_SECONDS: int = 86_400

    # Отдельный Redis для cache-aside ответа POST /features. Пусто = кэш ответа выключен.
    FEATURE_RESPONSE_CACHE_URL: str = ""
    FEATURE_RESPONSE_CACHE_TTL_SECONDS: int = 1800


settings = Settings()


def get_keydb_url_by_version(version: str = None) -> str:
    v = version or settings.STORAGE_VERSION
    if v == "v1":
        return settings.KEYDB_V1_URL
    elif v == "v2":
        return settings.KEYDB_V2_URL
    elif v == "v3":
        return settings.KEYDB_V3_URL
    else:
        raise ValueError(f"Unknown storage version: {v}")

