"""Оркестратор сборки фич через абстракцию FeatureRepository."""

from __future__ import annotations

import logging
from typing import Any

from app.api.v1.schemas.request import FeatureRequest
from app.api.v1.schemas.response import FeatureResponse, FeaturesBlock, MetadataResponse
from app.repositories.base import FeatureRepository
from app.cache.pers_cols_cache import PersColsCache
from app.services.validation.feature_rules import FEATURE_WHITELIST_BY_GROUP

logger = logging.getLogger(__name__)

class FeatureOrchestrationService:
    def __init__(
        self,
        repository: FeatureRepository,
        pers_cols_cache: PersColsCache,
    ) -> None:
        self._repo = repository
        self._pers_cols_cache = pers_cols_cache

    async def fetch(self, request: FeatureRequest) -> FeatureResponse:
        rf = request.requested_features
        groups: set[str] = set()
        if rf.pers_user_item is not None:
            groups.add("pers_user_item")
        if rf.pers_item is not None:
            groups.add("pers_item")
        if rf.pers_offl is not None:
            groups.add("pers_offl")

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

        entry0 = request.entries[0]
        items = request.items
        retail_brand = request.brand.value if request.brand else ""
        user_id = entry0.user_id
        store_id = entry0.store_id

        city_id, user_cities = await self._resolve_city(store_id, user_id)

        aggregated_cities: list[int] = []
        if city_id != -1:
            aggregated_cities = [city_id]
        elif user_cities:
            aggregated_cities = list(user_cities)

        features_block = FeaturesBlock()

        need_pui = "pers_user_item" in groups
        need_pi = "pers_item" in groups
        need_offl = "pers_offl" in groups

        all_names = await self._pers_cols_cache.get(self._repo)
        pui_cols = all_names.get("pers_user_item", [])
        pi_cols = all_names.get("pers_item", [])

        if need_pui and user_id is not None and retail_brand:
            pui_rows: list[dict[int, list[Any]]] = []
            if city_id != -1:
                pui_rows.append(
                    await self._repo.get_pers_user_item(
                        retail_brand, user_id, city_id, columns=pui_cols
                    )
                )
            elif user_cities:
                for c in user_cities:
                    pui_rows.append(
                        await self._repo.get_pers_user_item(
                            retail_brand, user_id, c, columns=pui_cols
                        )
                    )
            else:
                pui_rows.append(
                    await self._repo.get_pers_user_item(
                        retail_brand, user_id, -1, columns=pui_cols
                    )
                )
            features_block.pers_user_item = self._build_pers_user_item(
                pui_rows,
                all_names.get("pers_user_item", []),
                rf.pers_user_item,
                items,
            )

        if need_pi and retail_brand:
            pi = await self._repo.get_pers_item_by_items(
                retail_brand,
                city_id,
                items,
                columns=pi_cols,
            )
            features_block.pers_item = self._build_pers_item_ordered(
                pi,
                items,
                all_names.get("pers_item", []),
                rf.pers_item,
            )

        if need_offl and user_id is not None:
            offl = await self._repo.get_pers_offl(user_id)
            features_block.pers_offl = self._build_pers_offl(
                offl,
                all_names.get("pers_offl", []),
                rf.pers_offl,
                items,
            )

        return FeatureResponse(
            features=features_block,
            metadata=MetadataResponse(aggregated_cities=aggregated_cities),
        )

    async def _resolve_city(
        self,
        store_id: int | None,
        user_id: int | None,
    ) -> tuple[int, list[int]]:
        city_id = -1
        user_cities: list[int] = []
        if store_id is not None and store_id != -1:
            found = await self._repo.get_store_city(store_id)
            if found is not None:
                city_id = found
        if city_id == -1 and user_id is not None:
            user_cities = await self._repo.get_user_cities(user_id)
        return city_id, user_cities

    def _build_pers_item_ordered(
        self,
        raw_rows: dict[int, list[Any]],
        items: list[int],
        col_names: list[str],
        requested: list[str] | None,
    ) -> dict[str, dict[str, Any]]:
        allowed = FEATURE_WHITELIST_BY_GROUP.get("pers_item", frozenset())
        idx, names = self._column_subset(col_names, requested, allowed)
        out: dict[str, dict[str, Any]] = {}
        for it in items:
            raw = raw_rows.get(it)
            if raw is None:
                out[str(it)] = {}
                continue
            values = self._extract_values(raw, names, idx)
            values = self._normalize_row(values, len(names), item_id=it)
            row = {names[i]: values[i] for i in range(len(names))}
            out[str(it)] = row
        return out

    def _build_pers_user_item(
        self,
        raw_hashes: list[dict[int, list[Any]]],
        col_names: list[str],
        requested: list[str] | None,
        items_filter: list[int],
    ) -> dict[str, dict[str, Any]]:
        allowed = FEATURE_WHITELIST_BY_GROUP.get("pers_user_item", frozenset())
        items_set = set(items_filter)
        nonempty = [h for h in raw_hashes if h]
        if not nonempty:
            return {}
        idx, names = self._column_subset(col_names, requested, allowed)
        feat_n = len(names)

        def one_hash(h: dict[int, list[Any]]) -> dict[int, list[Any]]:
            return self._filter_hash_rows(h, items_set, idx, names)

        acc = one_hash(nonempty[0])
        for h in nonempty[1:]:
            nxt = one_hash(h)
            acc = {
                item: [
                    self._at(acc.get(item, []), i) + self._at(nxt.get(item, []), i)
                    for i in range(feat_n)
                ]
                for item in acc.keys() | nxt.keys()
            }
        out: dict[str, dict[str, Any]] = {}
        for k, row in acc.items():
            row_norm = self._normalize_row(row, feat_n, item_id=k)
            out[str(k)] = {names[i]: row_norm[i] for i in range(feat_n)}
        return out

    def _filter_hash_rows(
        self,
        feats_values: dict[int, list[Any]],
        items_set: set[int],
        idx: list[int],
        names: list[str] | None,
    ) -> dict[int, list[Any]]:
        result: dict[int, list[Any]] = {}
        for item_i, feats in feats_values.items():
            if item_i not in items_set:
                continue
            values = self._extract_values(feats, names=names, idx=idx)
            target_len = len(names) if names else len(idx)
            result[item_i] = self._normalize_row(values, target_len, item_id=item_i)
        return result

    def _build_pers_offl(
        self,
        feats_values: dict[int, list[Any]],
        col_names: list[str],
        requested: list[str] | None,
        items: list[int],
    ) -> dict[str, dict[str, Any]]:
        allowed = FEATURE_WHITELIST_BY_GROUP.get("pers_offl", frozenset())
        idx, names = self._column_subset(col_names, requested, allowed)
        items_set = set(items)
        out: dict[str, dict[str, Any]] = {}
        for item_i, feats in feats_values.items():
            if item_i not in items_set:
                continue
            values = self._extract_values(feats, names, idx)
            values = self._normalize_row(values, len(names), item_id=item_i)
            out[str(item_i)] = {names[i]: values[i] for i in range(len(names))}
        return out

    def _column_subset(
        self,
        col_names: list[str],
        requested: list[str] | None,
        allowed: frozenset[str],
    ) -> tuple[list[int], list[str]]:
        if requested is None or len(requested) == 0:
            pick = [c for c in col_names if c in allowed]
        else:
            pick = [c for c in requested if c in allowed and c in col_names]
        idx_list = [col_names.index(c) for c in pick]
        return idx_list, pick

    @staticmethod
    def _extract_values(
        feats: Any, names: list[str] | None, idx: list[int]
    ) -> list[Any]:
        """
        Совместимость форматов:
        - list/tuple: доступ по индексам (v1/v2, часть v3)
        - dict: доступ по именам колонок (v3 pers_item/pers_user_item)
        """
        if isinstance(feats, dict):
            if not names:
                return []
            return [feats.get(name, 0) for name in names]
        values: list[Any] = []
        for i in idx:
            values.append(feats[i] if i < len(feats) else 0)
        return values

    @staticmethod
    def _at(row: list[Any], i: int) -> Any:
        return row[i] if i < len(row) else 0

    @staticmethod
    def _normalize_row(row: list[Any], target_len: int, item_id: int | None = None) -> list[Any]:
        if len(row) == target_len:
            return row
        if item_id is not None:
            logger.warning(
                "Feature vector length mismatch for item %s: got=%s expected=%s",
                item_id,
                len(row),
                target_len,
            )
        if len(row) < target_len:
            return row + [0] * (target_len - len(row))
        return row[:target_len]

