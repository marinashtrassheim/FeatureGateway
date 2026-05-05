"""Контекст одного запроса признаков (после базовых проверок тела)."""

from __future__ import annotations

from dataclasses import dataclass

from app.api.v1.schemas.request import FeatureRequest, RequestedFeatures


@dataclass(frozen=True, slots=True)
class FeatureAccessContext:
    groups: frozenset[str]
    items: list[int]
    retail_brand: str
    user_id: int | None
    store_id: int | None
    requested_features: RequestedFeatures


def build_feature_access_context(request: FeatureRequest) -> FeatureAccessContext:
    rf = request.requested_features
    groups: set[str] = set()
    if rf.pers_user_item is not None:
        groups.add("pers_user_item")
    if rf.pers_item is not None:
        groups.add("pers_item")
    if rf.pers_offl is not None:
        groups.add("pers_offl")

    entry0 = request.entries[0]
    return FeatureAccessContext(
        groups=frozenset(groups),
        items=list(request.items or []),
        retail_brand=request.brand.value if request.brand else "",
        user_id=entry0.user_id,
        store_id=entry0.store_id,
        requested_features=rf,
    )
