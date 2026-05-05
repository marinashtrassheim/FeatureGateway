"""Извлечение и нормализация векторов признаков (общая логика сборщиков)."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def extract_feature_values(
    feats: Any, names: list[str] | None, idx: list[int]
) -> list[Any]:
    values: list[Any] = []
    for i in idx:
        values.append(feats[i] if i < len(feats) else 0)
    return values


def feature_row_at(row: list[Any], i: int) -> Any:
    return row[i] if i < len(row) else 0


def normalize_feature_row(
    row: list[Any], target_len: int, item_id: int | None = None
) -> list[Any]:
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
