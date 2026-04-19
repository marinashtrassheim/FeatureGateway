"""In-process кэш pers_cols без Redis."""

from __future__ import annotations

import pytest

from app.cache.pers_cols_cache import PersColsCache

from tests.conftest import FakeFeatureRepository


@pytest.mark.asyncio
async def test_second_get_within_ttl_does_not_call_get_pers_cols_again() -> None:
    repo = FakeFeatureRepository(
        pers_cols={
            "pers_user_item": ["a"],
            "pers_item": ["b"],
            "pers_offl": ["c"],
        }
    )
    cache = PersColsCache(ttl_seconds=3600)
    first = await cache.get(repo)
    second = await cache.get(repo)
    assert first is second
    assert repo.get_pers_cols_calls == 1
