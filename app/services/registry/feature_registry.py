"""Единая точка каталога публичных и внутренних групп признаков."""

from __future__ import annotations

from typing import Final

from app.services.validation.feature_rules import (
    FEATURE_WHITELIST_BY_GROUP,
    INTERNAL_FEATURE_WHITELIST,
)


class FeatureRegistry:
    """Whitelist имён по группам и сопоставление с порядком колонок из pers_cols."""

    _PUBLIC: Final[dict[str, frozenset[str]]] = FEATURE_WHITELIST_BY_GROUP
    _INTERNAL: Final[dict[str, frozenset[str]]] = INTERNAL_FEATURE_WHITELIST

    def list_groups(self) -> list[str]:
        return sorted(self._PUBLIC.keys())

    def allowed_names(self, group: str) -> frozenset[str]:
        return self._PUBLIC.get(group, frozenset())

    def internal_groups(self) -> frozenset[str]:
        return frozenset(self._INTERNAL.keys())

    def internal_allowed_names(self, group: str) -> frozenset[str]:
        return self._INTERNAL.get(group, frozenset())

    def resolve_columns(
        self,
        group: str,
        requested: list[str] | None,
        pers_cols_order: list[str],
    ) -> tuple[list[int], list[str]]:
        allowed = self.allowed_names(group)
        if requested is None or len(requested) == 0:
            pick = [c for c in pers_cols_order if c in allowed]
        else:
            pick = [c for c in requested if c in allowed and c in pers_cols_order]
        idx_list = [pers_cols_order.index(c) for c in pick]
        return idx_list, pick
