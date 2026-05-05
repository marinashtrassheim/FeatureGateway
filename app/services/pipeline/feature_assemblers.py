"""Сборка ответа API из сырых строк репозитория (без I/O)."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from app.services.registry.feature_registry import FeatureRegistry
from app.services.pipeline.feature_row_utils import (
    extract_feature_values,
    feature_row_at,
    normalize_feature_row,
)


@runtime_checkable
class PersItemAssemblerProtocol(Protocol):
    def build(
        self,
        raw_rows: dict[int, list[Any]],
        items: list[int],
        col_names: list[str],
        requested: list[str] | None,
    ) -> dict[str, dict[str, Any]]:
        ...


@runtime_checkable
class PersUserItemAssemblerProtocol(Protocol):
    def build(
        self,
        raw_hashes: list[dict[int, list[Any]]],
        col_names: list[str],
        requested: list[str] | None,
        items_filter: list[int],
    ) -> dict[str, dict[str, Any]]:
        ...


@runtime_checkable
class PersOfflAssemblerProtocol(Protocol):
    def build(
        self,
        feats_values: dict[int, list[Any]],
        col_names: list[str],
        requested: list[str] | None,
        items: list[int],
    ) -> dict[str, dict[str, Any]]:
        ...


class PersItemAssembler:
    def __init__(self, registry: FeatureRegistry) -> None:
        self._registry = registry

    def build(
        self,
        raw_rows: dict[int, list[Any]],
        items: list[int],
        col_names: list[str],
        requested: list[str] | None,
    ) -> dict[str, dict[str, Any]]:
        idx, names = self._registry.resolve_columns(
            "pers_item", requested, col_names
        )
        out: dict[str, dict[str, Any]] = {}
        for it in items:
            raw = raw_rows.get(it)
            if raw is None:
                out[str(it)] = {}
                continue
            values = extract_feature_values(raw, names, idx)
            values = normalize_feature_row(values, len(names), item_id=it)
            row = {names[i]: values[i] for i in range(len(names))}
            out[str(it)] = row
        return out


class PersUserItemAssembler:
    def __init__(self, registry: FeatureRegistry) -> None:
        self._registry = registry

    def build(
        self,
        raw_hashes: list[dict[int, list[Any]]],
        col_names: list[str],
        requested: list[str] | None,
        items_filter: list[int],
    ) -> dict[str, dict[str, Any]]:
        items_set = set(items_filter)
        nonempty = [h for h in raw_hashes if h]
        if not nonempty:
            return {}
        idx, names = self._registry.resolve_columns(
            "pers_user_item", requested, col_names
        )
        feat_n = len(names)

        def one_hash(h: dict[int, list[Any]]) -> dict[int, list[Any]]:
            return self._filter_hash_rows(h, items_set, idx, names)

        acc = one_hash(nonempty[0])
        for h in nonempty[1:]:
            nxt = one_hash(h)
            acc = {
                item: [
                    feature_row_at(acc.get(item, []), i)
                    + feature_row_at(nxt.get(item, []), i)
                    for i in range(feat_n)
                ]
                for item in acc.keys() | nxt.keys()
            }
        out: dict[str, dict[str, Any]] = {}
        for k, row in acc.items():
            row_norm = normalize_feature_row(row, feat_n, item_id=k)
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
            values = extract_feature_values(feats, names=names, idx=idx)
            target_len = len(names) if names else len(idx)
            result[item_i] = normalize_feature_row(
                values, target_len, item_id=item_i
            )
        return result


class PersOfflAssembler:
    def __init__(self, registry: FeatureRegistry) -> None:
        self._registry = registry

    def build(
        self,
        feats_values: dict[int, list[Any]],
        col_names: list[str],
        requested: list[str] | None,
        items: list[int],
    ) -> dict[str, dict[str, Any]]:
        idx, names = self._registry.resolve_columns(
            "pers_offl", requested, col_names
        )
        items_set = set(items)
        out: dict[str, dict[str, Any]] = {}
        for item_i, feats in feats_values.items():
            if item_i not in items_set:
                continue
            values = extract_feature_values(feats, names, idx)
            values = normalize_feature_row(values, len(names), item_id=item_i)
            out[str(item_i)] = {names[i]: values[i] for i in range(len(names))}
        return out
