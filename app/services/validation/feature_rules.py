"""
Допустимые имена признаков по группам (совпадают с ключами в requested_features и в ответе).

pers_query_item — только для внутреннего маппинга шлюза, не валидируется как публичная группа.
"""

from __future__ import annotations

from typing import Final

FEATURE_WHITELIST_BY_GROUP: Final[dict[str, frozenset[str]]] = {
    "pers_item": frozenset(
        {
            "ord_365",
            "ord_60",
            "pei_60",
            "ord_14",
            "ord_365_total",
            "ord_60_total",
            "pei_60_total",
            "ord_14_total",
            "margin",
            "m2",
            "price",
            "discount_rub",
            "discount_prt",
            "m2_prt",
        }
    ),
    "pers_user_item": frozenset(
        {
            "pers_pei",
            "pers_ord",
            "pers_revenue",
            "min_price",
            "diff_coef",
            "wgh_pers_ord",
            "std_price",
            "diff_ord",
            "reg_b",
            "items_b",
            "rev_coef",
            "pei_coef",
            "perc_return",
            "perc_discount",
            "first_b",
            "last_b",
        }
    ),
    "pers_offl": frozenset({"offl_ord", "offl_pei"}),
}

# Внутренний маппинг (не в публичном requested_features)
INTERNAL_FEATURE_WHITELIST: Final[dict[str, frozenset[str]]] = {
    "pers_query_item": frozenset({"ctr_sess"}),
}
