"""
Правила запроса по группам признаков (brand, entries, обязательные поля в entry).

Новая группа: добавить запись в FEATURE_GROUP_RULES и поле в RequestedFeatures,
whitelist имён — в feature_rules.FEATURE_WHITELIST_BY_GROUP.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True, slots=True)
class FeatureGroupRule:
    """Декларативное описание требований к телу запроса для одной группы."""

    id: str
    requires_brand: bool
    requires_non_empty_entries: bool
    required_entry_fields: frozenset[str]


FEATURE_GROUP_RULES: Final[dict[str, FeatureGroupRule]] = {
    "pers_item": FeatureGroupRule(
        id="pers_item",
        requires_brand=True,
        requires_non_empty_entries=True,
        required_entry_fields=frozenset(),
    ),
    "pers_user_item": FeatureGroupRule(
        id="pers_user_item",
        requires_brand=True,
        requires_non_empty_entries=True,
        required_entry_fields=frozenset({"user_id"}),
    ),
    "pers_offl": FeatureGroupRule(
        id="pers_offl",
        requires_brand=False,
        requires_non_empty_entries=True,
        required_entry_fields=frozenset({"user_id"}),
    ),
}
