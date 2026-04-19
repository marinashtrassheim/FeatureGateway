from functools import lru_cache

from fastapi import Request

from app.core.config import settings
from app.repositories.base import FeatureRepository
from app.repositories.v1.repository import V1FeatureRepository
from app.repositories.v2.repository import V2FeatureRepository
from app.repositories.v3.repository import V3FeatureRepository
from app.services.feature_orchestration import FeatureOrchestrationService
from app.services.feature_request_validator import FeatureRequestValidator
from app.cache.pers_cols_cache import PersColsCache, build_pers_cols_cache


@lru_cache
def get_feature_request_validator() -> FeatureRequestValidator:
    return FeatureRequestValidator()


@lru_cache
def get_pers_cols_cache() -> PersColsCache:
    return build_pers_cols_cache(settings.PERS_COLS_CACHE_TTL_SECONDS)


def get_feature_repository(request: Request) -> FeatureRepository:
    version = settings.STORAGE_VERSION.strip().lower()
    if version == "v1":
        return V1FeatureRepository(request.app.state.keydb_ds, request.app.state.keydb_ds_second)
    if version == "v2":
        return V2FeatureRepository(request.app.state.keydb_ds, request.app.state.keydb_ds_second)
    if version == "v3":
        return V3FeatureRepository(request.app.state.keydb_ds, request.app.state.keydb_ds_second)
    raise ValueError(f"Unknown STORAGE_VERSION: {settings.STORAGE_VERSION}")


def get_feature_orchestration(request: Request) -> FeatureOrchestrationService:
    repository = get_feature_repository(request)
    return FeatureOrchestrationService(repository, get_pers_cols_cache())
