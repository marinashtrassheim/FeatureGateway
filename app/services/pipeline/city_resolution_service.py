"""Определение города: магазин → pers_hub_city, иначе список городов пользователя."""

from __future__ import annotations

from app.repositories.base import FeatureRepository


class CityResolutionService:
    async def resolve(
        self,
        repo: FeatureRepository,
        store_id: int | None,
        user_id: int | None,
    ) -> tuple[int, list[int]]:
        return await repo.resolve_city_context(store_id, user_id)
