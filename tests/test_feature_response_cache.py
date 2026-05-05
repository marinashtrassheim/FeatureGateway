from __future__ import annotations

from app.api.v1.schemas.request import FeatureRequest
from app.cache.feature_response_cache import (
    FEATURE_CONTRACT_VERSION,
    build_response_cache_key,
)


def _body(entries: list[dict]) -> FeatureRequest:
    return FeatureRequest.model_validate(
        {
            "brand": "lo",
            "items": [3, 1, 2],
            "entries": entries,
            "requested_features": {
                "pers_item": [],
                "pers_user_item": [],
            },
        }
    )


def test_cache_key_uses_contract_version_prefix() -> None:
    key = build_response_cache_key(_body([{"user_id": 1, "store_id": 10}]))
    assert key.startswith(f"gw:features:{FEATURE_CONTRACT_VERSION}:")


def test_cache_key_depends_only_on_first_entry() -> None:
    key1 = build_response_cache_key(
        _body([{"user_id": 1, "store_id": 10}, {"user_id": 999, "store_id": 999}])
    )
    key2 = build_response_cache_key(_body([{"user_id": 1, "store_id": 10}]))
    assert key1 == key2
