"""Unit-тесты FeatureOrchestrationService (фейковый репозиторий)."""

from __future__ import annotations

import pytest

from app.api.v1.schemas.request import FeatureEntry, FeatureRequest, RequestedFeatures
from app.core.constants import Brand
from app.services.feature_orchestration import FeatureOrchestrationService
from app.cache.pers_cols_cache import PersColsCache
from app.services.validation.feature_rules import FEATURE_WHITELIST_BY_GROUP

from tests.conftest import FakeFeatureRepository


def _svc(repo: FakeFeatureRepository) -> FeatureOrchestrationService:
    return FeatureOrchestrationService(repo, PersColsCache(ttl_seconds=86_400))


def _default_pers_cols() -> dict[str, list[str]]:
    return {
        "pers_user_item": list(FEATURE_WHITELIST_BY_GROUP["pers_user_item"])[:4],
        "pers_item": ["ord_60", "price", "margin"],
        "pers_offl": ["offl_ord", "offl_pei"],
    }


@pytest.mark.asyncio
async def test_early_return_empty_entries_no_repo_calls() -> None:
    repo = FakeFeatureRepository(pers_cols=_default_pers_cols())
    svc = _svc(repo)
    req = FeatureRequest(
        brand=Brand.LO,
        items=[1],
        entries=[],
        requested_features=RequestedFeatures(pers_item=[]),
    )
    await svc.fetch(req)
    assert repo.get_pers_cols_calls == 0
    assert repo.get_store_city_calls == 0


@pytest.mark.asyncio
async def test_early_return_empty_items_no_repo_calls() -> None:
    repo = FakeFeatureRepository(pers_cols=_default_pers_cols())
    svc = _svc(repo)
    req = FeatureRequest(
        brand=Brand.LO,
        items=None,
        entries=[FeatureEntry(user_id=1, store_id=10)],
        requested_features=RequestedFeatures(pers_item=[]),
    )
    await svc.fetch(req)
    assert repo.get_pers_cols_calls == 0


@pytest.mark.asyncio
async def test_only_pers_user_item_does_not_call_pers_item_or_offl() -> None:
    repo = FakeFeatureRepository(pers_cols=_default_pers_cols())
    repo.store_city = 77
    repo.pui_by_city[77] = {1: [1, 1, 1, 1]}
    svc = _svc(repo)
    req = FeatureRequest(
        brand=Brand.LO,
        items=[1],
        entries=[FeatureEntry(user_id=100, store_id=5)],
        requested_features=RequestedFeatures(pers_user_item=["pers_pei", "pers_ord"]),
    )
    await svc.fetch(req)
    assert len(repo.get_pers_item_by_items_calls) == 0
    assert len(repo.get_pers_offl_calls) == 0
    assert len(repo.get_pers_user_item_calls) >= 1


@pytest.mark.asyncio
async def test_only_pers_item_does_not_call_pui_or_offl() -> None:
    repo = FakeFeatureRepository(pers_cols=_default_pers_cols())
    repo.store_city = 3
    repo.pi_rows = {10: [1, 2, 3]}
    svc = _svc(repo)
    req = FeatureRequest(
        brand=Brand.LO,
        items=[10],
        entries=[FeatureEntry(user_id=1, store_id=5)],
        requested_features=RequestedFeatures(pers_item=["ord_60"]),
    )
    await svc.fetch(req)
    assert len(repo.get_pers_user_item_calls) == 0
    assert len(repo.get_pers_offl_calls) == 0
    assert len(repo.get_pers_item_by_items_calls) == 1


@pytest.mark.asyncio
async def test_only_pers_offl_does_not_call_pui_or_pi() -> None:
    repo = FakeFeatureRepository(pers_cols=_default_pers_cols())
    repo.offl_rows = {20: [7, 8]}
    svc = _svc(repo)
    req = FeatureRequest(
        brand=None,
        items=[20],
        entries=[FeatureEntry(user_id=999, store_id=None)],
        requested_features=RequestedFeatures(pers_offl=["offl_ord"]),
    )
    await svc.fetch(req)
    assert len(repo.get_pers_user_item_calls) == 0
    assert len(repo.get_pers_item_by_items_calls) == 0
    assert repo.get_pers_offl_calls == [999]


@pytest.mark.asyncio
async def test_store_id_resolves_single_city_one_pui_call() -> None:
    repo = FakeFeatureRepository(pers_cols=_default_pers_cols())
    repo.store_city = 100
    repo.pui_by_city[100] = {1: [1, 0, 0, 0]}
    svc = _svc(repo)
    req = FeatureRequest(
        brand=Brand.LO,
        items=[1],
        entries=[FeatureEntry(user_id=50, store_id=200)],
        requested_features=RequestedFeatures(pers_user_item=["pers_pei"]),
    )
    await svc.fetch(req)
    assert repo.get_store_city_calls == 1
    assert len(repo.get_pers_user_item_calls) == 1
    assert repo.get_pers_user_item_calls[0][:3] == ("lo", 50, 100)


@pytest.mark.asyncio
async def test_no_store_uses_user_cities_multiple_pui_calls() -> None:
    repo = FakeFeatureRepository(pers_cols=_default_pers_cols())
    repo.store_city = None
    repo.user_cities = [10, 20]
    repo.pui_by_city[10] = {5: [2, 0, 0, 0]}
    repo.pui_by_city[20] = {5: [3, 0, 0, 0]}
    svc = _svc(repo)
    req = FeatureRequest(
        brand=Brand.LO,
        items=[5],
        entries=[FeatureEntry(user_id=1, store_id=None)],
        requested_features=RequestedFeatures(pers_user_item=["pers_pei"]),
    )
    await svc.fetch(req)
    assert repo.get_user_cities_calls == 1
    assert len(repo.get_pers_user_item_calls) == 2
    cities_called = [c[2] for c in repo.get_pers_user_item_calls]
    assert set(cities_called) == {10, 20}


@pytest.mark.asyncio
async def test_no_store_no_user_cities_fallback_city_minus_one() -> None:
    repo = FakeFeatureRepository(pers_cols=_default_pers_cols())
    repo.store_city = None
    repo.user_cities = []
    repo.pui_by_city[-1] = {7: [1, 0, 0, 0]}
    svc = _svc(repo)
    req = FeatureRequest(
        brand=Brand.LO,
        items=[7],
        entries=[FeatureEntry(user_id=2, store_id=None)],
        requested_features=RequestedFeatures(pers_user_item=["pers_pei"]),
    )
    await svc.fetch(req)
    assert len(repo.get_pers_user_item_calls) == 1
    assert repo.get_pers_user_item_calls[0][2] == -1


@pytest.mark.asyncio
async def test_two_cities_pui_values_summed() -> None:
    cols = ["pers_pei", "pers_ord"]
    repo = FakeFeatureRepository(
        pers_cols={
            "pers_user_item": cols,
            "pers_item": ["ord_60"],
            "pers_offl": ["offl_ord"],
        }
    )
    repo.user_cities = [10, 20]
    repo.pui_by_city[10] = {100: [1.0, 10.0]}
    repo.pui_by_city[20] = {100: [3.0, 5.0]}
    svc = _svc(repo)
    req = FeatureRequest(
        brand=Brand.LO,
        items=[100],
        entries=[FeatureEntry(user_id=1, store_id=None)],
        requested_features=RequestedFeatures(pers_user_item=cols),
    )
    resp = await svc.fetch(req)
    row = resp.features.pers_user_item or {}
    assert row["100"]["pers_pei"] == 4.0
    assert row["100"]["pers_ord"] == 15.0
    assert set(resp.metadata.aggregated_cities) == {10, 20}


@pytest.mark.asyncio
async def test_columns_passed_to_repo_match_pers_cols_cache() -> None:
    pc = _default_pers_cols()
    repo = FakeFeatureRepository(pers_cols=pc)
    repo.store_city = 1
    repo.pui_by_city[1] = {1: [0]}
    repo.pi_rows = {2: [0, 0, 0]}
    svc = _svc(repo)
    req = FeatureRequest(
        brand=Brand.LO,
        items=[1, 2],
        entries=[FeatureEntry(user_id=1, store_id=5)],
        requested_features=RequestedFeatures(
            pers_user_item=["pers_pei"],
            pers_item=["ord_60"],
        ),
    )
    await svc.fetch(req)
    assert repo.get_pers_user_item_calls
    assert repo.get_pers_item_by_items_calls
    assert repo.get_pers_user_item_calls[0][3] == pc["pers_user_item"]
    assert repo.get_pers_item_by_items_calls[0][3] == pc["pers_item"]


class TestOrchestrationHelpers:
    """_column_subset и _normalize_row без KeyDB."""

    def test_column_subset_requested_filters_whitelist_and_col_names(self) -> None:
        repo = FakeFeatureRepository()
        svc = _svc(repo)
        col_names = ["ord_60", "price", "margin"]
        allowed = FEATURE_WHITELIST_BY_GROUP["pers_item"]
        idx, names = svc._column_subset(
            col_names,
            ["ord_60", "unknown_feature", "price"],
            allowed,
        )
        assert names == ["ord_60", "price"]
        # порядок как в requested; индексы — позиции в col_names (price — второй столбец)
        assert idx == [0, 1]

    def test_column_subset_empty_requested_uses_intersection_with_allowed(self) -> None:
        repo = FakeFeatureRepository()
        svc = _svc(repo)
        col_names = ["ord_60", "price", "not_in_whitelist"]
        allowed = FEATURE_WHITELIST_BY_GROUP["pers_item"]
        idx, names = svc._column_subset(col_names, None, allowed)
        assert "not_in_whitelist" not in names
        assert "ord_60" in names

    def test_normalize_row_pads_short_vector(self) -> None:
        repo = FakeFeatureRepository()
        svc = _svc(repo)
        out = svc._normalize_row([1], 3, item_id=1)
        assert out == [1, 0, 0]

    def test_normalize_row_truncates_long_vector(self) -> None:
        repo = FakeFeatureRepository()
        svc = _svc(repo)
        out = svc._normalize_row([1, 2, 3, 4], 2, item_id=1)
        assert out == [1, 2]
