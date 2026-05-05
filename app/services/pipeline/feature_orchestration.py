"""Оркестратор pipeline: контекст → город → план → загрузка → сборка ответа."""

from __future__ import annotations

from app.api.v1.schemas.request import FeatureRequest
from app.api.v1.schemas.response import FeatureResponse, FeaturesBlock, MetadataResponse
from app.repositories.base import FeatureRepository
from app.cache.pers_cols_cache import PersColsCache
from app.services.pipeline.city_resolution_service import CityResolutionService
from app.services.pipeline.feature_access_context import build_feature_access_context
from app.services.pipeline.feature_assemblers import (
    PersItemAssembler,
    PersOfflAssembler,
    PersUserItemAssembler,
)
from app.services.pipeline.feature_fetch_plan import build_feature_fetch_plan
from app.services.pipeline.feature_loaders import PersItemLoader, PersOfflLoader, PersUserItemLoader
from app.services.registry.feature_registry import FeatureRegistry


class FeatureOrchestrationService:
    def __init__(
        self,
        repository: FeatureRepository,
        pers_cols_cache: PersColsCache,
        *,
        registry: FeatureRegistry | None = None,
        city_resolution: CityResolutionService | None = None,
        pui_loader: PersUserItemLoader | None = None,
        pi_loader: PersItemLoader | None = None,
        offl_loader: PersOfflLoader | None = None,
        pers_item_assembler: PersItemAssembler | None = None,
        pers_user_item_assembler: PersUserItemAssembler | None = None,
        pers_offl_assembler: PersOfflAssembler | None = None,
    ) -> None:
        self._repo = repository
        self._pers_cols_cache = pers_cols_cache
        self._registry = registry or FeatureRegistry()
        self._city = city_resolution or CityResolutionService()
        self._pui_loader = pui_loader or PersUserItemLoader()
        self._pi_loader = pi_loader or PersItemLoader()
        self._offl_loader = offl_loader or PersOfflLoader()
        self._pi_asm = pers_item_assembler or PersItemAssembler(self._registry)
        self._pui_asm = pers_user_item_assembler or PersUserItemAssembler(
            self._registry
        )
        self._offl_asm = pers_offl_assembler or PersOfflAssembler(self._registry)

    async def fetch(self, request: FeatureRequest) -> FeatureResponse:
        if not request.entries:
            return FeatureResponse(
                features=FeaturesBlock(),
                metadata=MetadataResponse(aggregated_cities=[]),
            )
        if not request.items:
            return FeatureResponse(
                features=FeaturesBlock(),
                metadata=MetadataResponse(aggregated_cities=[]),
            )

        ctx = build_feature_access_context(request)

        city_id, user_cities = await self._city.resolve(
            self._repo, ctx.store_id, ctx.user_id
        )

        aggregated_cities: list[int] = []
        if city_id != -1:
            aggregated_cities = [city_id]
        elif user_cities:
            aggregated_cities = list(user_cities)

        plan = build_feature_fetch_plan(ctx, city_id, user_cities)

        features_block = FeaturesBlock()

        all_names = await self._pers_cols_cache.get(self._repo)

        rf = ctx.requested_features

        if plan.pui_city_ids_in_order:
            assert ctx.user_id is not None
            pui_rows = await self._pui_loader.load(
                self._repo,
                brand=ctx.retail_brand,
                user_id=ctx.user_id,
                city_ids_in_order=plan.pui_city_ids_in_order,
            )
            features_block.pers_user_item = self._pui_asm.build(
                pui_rows,
                all_names.get("pers_user_item", []),
                rf.pers_user_item,
                ctx.items,
            )

        if plan.load_pers_item:
            pi = await self._pi_loader.load(
                self._repo,
                brand=ctx.retail_brand,
                city_id=city_id,
                items=ctx.items,
            )
            features_block.pers_item = self._pi_asm.build(
                pi,
                ctx.items,
                all_names.get("pers_item", []),
                rf.pers_item,
            )

        if plan.load_pers_offl:
            assert ctx.user_id is not None
            offl = await self._offl_loader.load(self._repo, ctx.user_id)
            features_block.pers_offl = self._offl_asm.build(
                offl,
                all_names.get("pers_offl", []),
                rf.pers_offl,
                ctx.items,
            )

        return FeatureResponse(
            features=features_block,
            metadata=MetadataResponse(aggregated_cities=aggregated_cities),
        )
