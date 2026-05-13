from __future__ import annotations

from typing import Any, Protocol


class FeatureRepository(Protocol):
    async def resolve_city_context(
        self, store_id: int | None, user_id: int | None
    ) -> tuple[int, list[int]]:
        """Город ТЦ и/или список городов пользователя (один round-trip к основному KeyDB, где возможно)."""

    async def get_store_city(self, store_id: int) -> int | None: ...

    async def get_user_cities(self, user_id: int) -> list[int]: ...

    async def get_pers_cols(self) -> dict[str, list[str]]: ...

    async def fetch_primary_pui_pi_bundle(
        self,
        *,
        brand: str,
        user_id: int | None,
        pers_item_city_id: int,
        items: list[int],
        pui_city_ids: tuple[int, ...],
    ) -> tuple[list[dict[int, list[Any]]], dict[int, list[Any]]]:
        """Один pipeline: все pers_user_item по городам + все HGET pers_item по items."""

    async def get_pers_user_item(
        self,
        brand: str,
        user_id: int,
        city_id: int,
    ) -> dict[int, list[Any]]: ...

    async def get_pers_item_by_items(
        self,
        brand: str,
        city_id: int,
        items: list[int],
    ) -> dict[int, list[Any]]: ...

    async def get_pers_offl(self, user_id: int) -> dict[int, list[Any]]: ...
