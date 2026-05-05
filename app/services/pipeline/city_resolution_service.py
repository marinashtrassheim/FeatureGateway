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
        city_id = -1
        user_cities: list[int] = []
        if store_id is not None and store_id != -1:
            found = await repo.get_store_city(store_id)
            if found is not None:
                city_id = found
        if city_id == -1 and user_id is not None:
            user_cities = await repo.get_user_cities(user_id)
        return city_id, user_cities
