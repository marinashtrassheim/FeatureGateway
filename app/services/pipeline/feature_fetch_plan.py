"""План чтения из KeyDB: порядок городов для pers_user_item и флаги загрузки."""

from __future__ import annotations

from dataclasses import dataclass

from app.services.pipeline.feature_access_context import FeatureAccessContext


@dataclass(frozen=True, slots=True)
class FeatureFetchPlan:
    """Пустой pui_city_ids_in_order означает «не вызывать get_pers_user_item»."""

    pui_city_ids_in_order: tuple[int, ...]
    load_pers_item: bool
    load_pers_offl: bool


def build_feature_fetch_plan(
    ctx: FeatureAccessContext,
    city_id: int,
    user_cities: list[int],
) -> FeatureFetchPlan:
    pui_cities: list[int] = []
    if (
        "pers_user_item" in ctx.groups
        and ctx.user_id is not None
        and ctx.retail_brand
    ):
        if city_id != -1:
            pui_cities = [city_id]
        elif user_cities:
            pui_cities = list(user_cities)
        else:
            pui_cities = [-1]

    load_pi = "pers_item" in ctx.groups and bool(ctx.retail_brand)
    load_offl = "pers_offl" in ctx.groups and ctx.user_id is not None

    return FeatureFetchPlan(
        pui_city_ids_in_order=tuple(pui_cities),
        load_pers_item=load_pi,
        load_pers_offl=load_offl,
    )
