"""Оркестратор pipeline: контекст → город → план → загрузка → сборка ответа."""

from __future__ import annotations

import asyncio
from typing import Any

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
from app.services.pipeline.feature_loaders import PersOfflLoader
from app.services.registry.feature_registry import FeatureRegistry


class FeatureOrchestrationService:
    def __init__(
        self,
        repository: FeatureRepository,
        pers_cols_cache: PersColsCache,
        *,
        registry: FeatureRegistry | None = None,
        city_resolution: CityResolutionService | None = None,
        offl_loader: PersOfflLoader | None = None,
        pers_item_assembler: PersItemAssembler | None = None,
        pers_user_item_assembler: PersUserItemAssembler | None = None,
        pers_offl_assembler: PersOfflAssembler | None = None,
    ) -> None:
        self._repo = repository
        self._pers_cols_cache = pers_cols_cache
        self._registry = registry or FeatureRegistry()
        self._city = city_resolution or CityResolutionService()
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

        need_primary_bundle = bool(plan.pui_city_ids_in_order) or plan.load_pers_item
        offl_needed = plan.load_pers_offl

        primary_pui_rows: list[dict[int, list[Any]]] | None = None
        primary_pi: dict[int, list[Any]] | None = None
        offl: dict[int, list[Any]] | None = None

        bundle_coro = None
        if need_primary_bundle:
            pui_ids = plan.pui_city_ids_in_order if plan.pui_city_ids_in_order else tuple()
            pi_items = ctx.items if plan.load_pers_item else []
            bundle_coro = self._repo.fetch_primary_pui_pi_bundle(
                brand=ctx.retail_brand,
                user_id=ctx.user_id,
                pers_item_city_id=city_id,
                items=pi_items,
                pui_city_ids=pui_ids,
            )

        offl_coro = None
        if offl_needed:
            assert ctx.user_id is not None
            offl_coro = self._offl_loader.load(self._repo, ctx.user_id)

        if bundle_coro is not None and offl_coro is not None:
            (primary_pui_rows, primary_pi), offl = await asyncio.gather(
                bundle_coro, offl_coro
            )
        elif bundle_coro is not None:
            primary_pui_rows, primary_pi = await bundle_coro
        elif offl_coro is not None:
            offl = await offl_coro

        if plan.pui_city_ids_in_order:
            assert ctx.user_id is not None
            assert primary_pui_rows is not None
            features_block.pers_user_item = self._pui_asm.build(
                primary_pui_rows,
                all_names.get("pers_user_item", []),
                rf.pers_user_item,
                ctx.items,
            )

        if plan.load_pers_item:
            assert primary_pi is not None
            features_block.pers_item = self._pi_asm.build(
                primary_pi,
                ctx.items,
                all_names.get("pers_item", []),
                rf.pers_item,
            )

        if plan.load_pers_offl:
            assert ctx.user_id is not None
            assert offl is not None
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
