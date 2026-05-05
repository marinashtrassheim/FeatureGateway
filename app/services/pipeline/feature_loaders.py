"""Загрузка сырых данных из FeatureRepository по шагам pipeline."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from app.repositories.base import FeatureRepository


@runtime_checkable
class PersUserItemLoaderProtocol(Protocol):
    async def load(
        self,
        repo: FeatureRepository,
        *,
        brand: str,
        user_id: int,
        city_ids_in_order: tuple[int, ...],
    ) -> list[dict[int, list[Any]]]:
        ...


@runtime_checkable
class PersItemLoaderProtocol(Protocol):
    async def load(
        self,
        repo: FeatureRepository,
        *,
        brand: str,
        city_id: int,
        items: list[int],
    ) -> dict[int, list[Any]]:
        ...


@runtime_checkable
class PersOfflLoaderProtocol(Protocol):
    async def load(
        self, repo: FeatureRepository, user_id: int
    ) -> dict[int, list[Any]]:
        ...


class PersUserItemLoader:
    async def load(
        self,
        repo: FeatureRepository,
        *,
        brand: str,
        user_id: int,
        city_ids_in_order: tuple[int, ...],
    ) -> list[dict[int, list[Any]]]:
        rows: list[dict[int, list[Any]]] = []
        for city_id in city_ids_in_order:
            rows.append(
                await repo.get_pers_user_item(brand, user_id, city_id)
            )
        return rows


class PersItemLoader:
    async def load(
        self,
        repo: FeatureRepository,
        *,
        brand: str,
        city_id: int,
        items: list[int],
    ) -> dict[int, list[Any]]:
        return await repo.get_pers_item_by_items(brand, city_id, items)


class PersOfflLoader:
    async def load(
        self, repo: FeatureRepository, user_id: int
    ) -> dict[int, list[Any]]:
        return await repo.get_pers_offl(user_id)
