"""Склейка POST /api/v1/features → deps → оркестратор (без KeyDB)."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from starlette.testclient import TestClient

from app.api.deps import get_feature_orchestration, get_feature_request_validator
from app.api.v1.endpoints.features import router as features_router
from app.api.v1.schemas.request import FeatureRequest
from app.api.v1.schemas.response import FeatureResponse, FeaturesBlock, MetadataResponse
from app.core.exceptions import (
    FeatureStorageError,
    FeatureStorageUnavailableError,
    FeatureValidationError,
)
from app.main import (
    feature_storage_handler,
    feature_validation_handler,
    pydantic_validation_handler,
)

from tests.conftest import make_minimal_request_body


class StaticOrchestration:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload
        self.last_request: FeatureRequest | None = None

    async def fetch(self, request: FeatureRequest) -> FeatureResponse:
        self.last_request = request
        return FeatureResponse.model_validate(self._payload)


class FailingOrchestration:
    async def fetch(self, request: FeatureRequest) -> FeatureResponse:
        raise FeatureStorageUnavailableError("KeyDB unavailable during test")


def _build_app(orchestration: Any) -> FastAPI:
    app = FastAPI()
    app.include_router(features_router, prefix="/api/v1")
    app.add_exception_handler(FeatureValidationError, feature_validation_handler)
    app.add_exception_handler(FeatureStorageError, feature_storage_handler)
    app.add_exception_handler(RequestValidationError, pydantic_validation_handler)

    def override_orch() -> Any:
        return orchestration

    app.dependency_overrides[get_feature_orchestration] = override_orch
    app.dependency_overrides[get_feature_request_validator] = get_feature_request_validator
    return app


def test_post_features_happy_path() -> None:
    response_body = {
        "features": {
            "pers_item": {"720704": {"ord_60": 1, "price": 2.0}},
            "pers_user_item": None,
            "pers_offl": None,
        },
        "metadata": {"aggregated_cities": [53]},
    }
    orch = StaticOrchestration(response_body)
    app = _build_app(orch)
    client = TestClient(app)

    body = make_minimal_request_body(
        groups={
            "pers_item": ["ord_60", "price"],
            "pers_user_item": ["pers_pei"],
        },
        entries=[{"user_id": 1, "store_id": 10}],
    )
    res = client.post("/api/v1/features", json=body)
    assert res.status_code == 200
    data = res.json()
    assert data["features"]["pers_item"]["720704"]["ord_60"] == 1
    assert orch.last_request is not None
    assert orch.last_request.brand is not None
    assert orch.last_request.brand.value == "lo"


def test_post_features_rejects_multiple_entries() -> None:
    response_body = {
        "features": {"pers_item": None, "pers_user_item": None, "pers_offl": None},
        "metadata": {"aggregated_cities": []},
    }
    orch = StaticOrchestration(response_body)
    app = _build_app(orch)
    client = TestClient(app)

    body = make_minimal_request_body(
        groups={"pers_item": ["ord_60"]},
        entries=[{"user_id": 1, "store_id": 10}, {"user_id": 2, "store_id": 20}],
    )
    res = client.post("/api/v1/features", json=body)
    assert res.status_code == 422
    payload = res.json()
    assert payload["code"] == "VALIDATION_ERROR"
    assert "ровно один контекст" in payload["message"]


def test_post_features_returns_503_on_storage_error() -> None:
    app = _build_app(FailingOrchestration())
    client = TestClient(app)
    body = make_minimal_request_body(
        groups={"pers_item": ["ord_60"]},
        entries=[{"user_id": 1, "store_id": 10}],
    )
    res = client.post("/api/v1/features", json=body)
    assert res.status_code == 503
    payload = res.json()
    assert payload["code"] == "FEATURE_STORAGE_UNAVAILABLE"
