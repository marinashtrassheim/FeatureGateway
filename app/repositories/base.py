from __future__ import annotations

from typing import Any, Protocol


class FeatureRepository(Protocol):
    async def get_store_city(self, store_id: int) -> int | None: ...

    async def get_user_cities(self, user_id: int) -> list[int]: ...

    async def get_pers_cols(self) -> dict[str, list[str]]: ...

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
