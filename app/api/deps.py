from functools import lru_cache

from fastapi import Request

from app.core.config import settings
from app.repositories.base import FeatureRepository
from app.repositories.feature_repository import KeyDbFeatureRepository
from app.services.pipeline.feature_orchestration import FeatureOrchestrationService
from app.services.registry.feature_registry import FeatureRegistry
from app.services.feature_request_validator import FeatureRequestValidator
from app.cache.pers_cols_cache import PersColsCache, build_pers_cols_cache


@lru_cache
def get_feature_registry() -> FeatureRegistry:
    return FeatureRegistry()


@lru_cache
def get_feature_request_validator() -> FeatureRequestValidator:
    return FeatureRequestValidator(get_feature_registry())


@lru_cache
def get_pers_cols_cache() -> PersColsCache:
    return build_pers_cols_cache(settings.PERS_COLS_CACHE_TTL_SECONDS)


def get_feature_repository(request: Request) -> FeatureRepository:
    return KeyDbFeatureRepository(request.app.state.keydb_ds, request.app.state.keydb_ds_second)


def get_feature_orchestration(request: Request) -> FeatureOrchestrationService:
    repository = get_feature_repository(request)
    return FeatureOrchestrationService(
        repository,
        get_pers_cols_cache(),
        registry=get_feature_registry(),
    )
