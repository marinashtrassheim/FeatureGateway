from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.core.constants import Brand


def make_minimal_request_body(
    *,
    groups: dict[str, list[str] | None],
    brand: str = "lo",
    items: list[int] | None = None,
    entries: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Тело POST /api/v1/features для интеграционных тестов."""
    if items is None:
        items = [720704]
    if entries is None:
        entries = [{"user_id": 1, "store_id": 10}]
    return {
        "brand": brand,
        "items": items,
        "entries": entries,
        "requested_features": groups,
    }


@dataclass
class FakeFeatureRepository:
    """Репозиторий-шпион для unit-тестов оркестратора."""

    store_city: int | None = 53
    user_cities: list[int] = field(default_factory=list)
    pers_cols: dict[str, list[str]] = field(default_factory=dict)

    get_pers_cols_calls: int = 0
    get_store_city_calls: int = 0
    get_user_cities_calls: int = 0
    resolve_city_context_calls: int = 0
    fetch_primary_pui_pi_bundle_calls: int = 0
    get_pers_user_item_calls: list[tuple[Any, ...]] = field(default_factory=list)
    get_pers_item_by_items_calls: list[tuple[Any, ...]] = field(default_factory=list)
    get_pers_offl_calls: list[int] = field(default_factory=list)

    pui_by_city: dict[int, dict[int, list[Any]]] = field(default_factory=dict)
    pi_rows: dict[int, list[Any]] = field(default_factory=dict)
    offl_rows: dict[int, list[Any]] = field(default_factory=dict)

    async def get_pers_cols(self) -> dict[str, list[str]]:
        self.get_pers_cols_calls += 1
        return self.pers_cols

    async def get_store_city(self, store_id: int) -> int | None:
        self.get_store_city_calls += 1
        return self.store_city

    async def get_user_cities(self, user_id: int) -> list[int]:
        self.get_user_cities_calls += 1
        return list(self.user_cities)

    async def resolve_city_context(
        self, store_id: int | None, user_id: int | None
    ) -> tuple[int, list[int]]:
        self.resolve_city_context_calls += 1
        city_id = -1
        user_cities: list[int] = []
        if store_id is not None and store_id != -1:
            found = await self.get_store_city(store_id)
            if found is not None:
                city_id = found
        if city_id == -1 and user_id is not None:
            user_cities = await self.get_user_cities(user_id)
        return city_id, user_cities

    async def fetch_primary_pui_pi_bundle(
        self,
        *,
        brand: str,
        user_id: int | None,
        pers_item_city_id: int,
        items: list[int],
        pui_city_ids: tuple[int, ...],
    ) -> tuple[list[dict[int, list[Any]]], dict[int, list[Any]]]:
        self.fetch_primary_pui_pi_bundle_calls += 1
        pui_rows: list[dict[int, list[Any]]] = []
        for cid in pui_city_ids:
            assert user_id is not None
            pui_rows.append(await self.get_pers_user_item(brand, user_id, cid))
        if items:
            pi = await self.get_pers_item_by_items(brand, pers_item_city_id, list(items))
        else:
            pi = {}
        return pui_rows, pi

    async def get_pers_user_item(
        self,
        brand: str,
        user_id: int,
        city_id: int,
    ) -> dict[int, list[Any]]:
        self.get_pers_user_item_calls.append((brand, user_id, city_id))
        return dict(self.pui_by_city.get(city_id, {}))

    async def get_pers_item_by_items(
        self,
        brand: str,
        city_id: int,
        items: list[int],
    ) -> dict[int, list[Any]]:
        self.get_pers_item_by_items_calls.append((brand, city_id, tuple(items)))
        return {k: v for k, v in self.pi_rows.items() if k in items}

    async def get_pers_offl(self, user_id: int) -> dict[int, list[Any]]:
        self.get_pers_offl_calls.append(user_id)
        return dict(self.offl_rows)
