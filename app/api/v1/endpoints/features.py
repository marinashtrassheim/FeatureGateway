from __future__ import annotations

from typing import Annotated
import json
import logging
from time import perf_counter
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Request

from app.api.deps import get_feature_orchestration, get_feature_request_validator
from app.api.v1.schemas.request import FeatureRequest
from app.api.v1.schemas.response import FeatureResponse
from app.services.pipeline.feature_orchestration import FeatureOrchestrationService
from app.services.feature_request_validator import FeatureRequestValidator

router = APIRouter(tags=["features"])


@router.post(
    "/features",
    response_model=FeatureResponse,
    summary="Получение feature из KeyDB",
    description=(
        "Универсальный запрос на получение `pers_item`, `pers_user_item`, `pers_offl` из KeyDB.\n\n"
        "- `brand`: бренд (`lo`, `mntk`, `utk`).\n"
        "- `items`: обязательный список `item_id` для фильтрации.\n"
        "- `entries`: сейчас поддерживается ровно один контекст (`len(entries) == 1`).\n"
        "- `entries[0].user_id`: обязателен для `pers_user_item` и `pers_offl`.\n"
        "- `entries[0].store_id`: опционален, при передаче город определяется через `pers_hub_city`.\n"
        "- В `requested_features`:\n"
        "  - `[]` означает \"все доступные поля группы\";\n"
        "  - отсутствие ключа означает \"группу не запрашивать\"."
    ),
    responses={
        422: {
            "description": "Ошибка валидации тела запроса (формат или бизнес-правила).",
            "content": {
                "application/json": {
                    "examples": {
                        "unknown_feature_name": {
                            "summary": "Запрошено несуществующее поле",
                            "value": {
                                "code": "VALIDATION_ERROR",
                                "message": "Ошибка валидации запроса",
                                "errors": [
                                    {
                                        "loc": [
                                            "body",
                                            "requested_features",
                                            "pers_user_item",
                                            0,
                                        ],
                                        "msg": "Для группы pers_user_item поле reg_w не существует в хранилище.",
                                    }
                                ],
                            },
                        },
                        "missing_user_id": {
                            "summary": "Не передан обязательный user_id",
                            "value": {
                                "code": "VALIDATION_ERROR",
                                "message": "Ошибка валидации запроса",
                                "errors": [
                                    {
                                        "loc": ["body", "entries", 0, "user_id"],
                                        "msg": "Для группы pers_user_item обязательно укажите user_id.",
                                    }
                                ],
                            },
                        },
                        "invalid_body_format": {
                            "summary": "Неверный формат поля",
                            "value": {
                                "code": "VALIDATION_ERROR",
                                "message": "Ошибка формата запроса",
                                "errors": [
                                    {
                                        "type": "int_parsing",
                                        "loc": ["body", "items", 0],
                                        "msg": "Input should be a valid integer, unable to parse string as an integer",
                                        "input": "abc",
                                    }
                                ],
                            },
                        },
                    }
                }
            },
        }
    },
)
async def post_features(
    request: Request,
    body: FeatureRequest,
    validator: Annotated[
        FeatureRequestValidator,
        Depends(get_feature_request_validator),
    ],
    orchestration: Annotated[
        FeatureOrchestrationService,
        Depends(get_feature_orchestration),
    ],
) -> FeatureResponse:
    """
    Универсальный запрос на фичи из KeyDB. После валидации — оркестратор загрузки.

    TIFUKNN и прочий не-KeyDB код моделей в шлюз не переносим (остаётся в модели).
    """
    logger = logging.getLogger("feature_gateway")
    start = perf_counter()

    groups: list[str] = []
    rf = body.requested_features
    if rf.pers_user_item is not None:
        groups.append("pers_user_item")
    if rf.pers_item is not None:
        groups.append("pers_item")
    if rf.pers_offl is not None:
        groups.append("pers_offl")

    response_cache_hit: bool | None = None
    try:
        validator.validate(body)
        cache = getattr(request.app.state, "feature_response_cache", None)
        if cache is not None and cache.enabled:
            cached = await cache.get(body)
            if cached is not None:
                result = cached
                response_cache_hit = True
            else:
                response_cache_hit = False
                result = await orchestration.fetch(body)
                await cache.set(body, result)
        else:
            result = await orchestration.fetch(body)
            response_cache_hit = None
        status = "OK"
        http_status = 200
    except Exception:
        # Исключение будет обработано выше по стеку, но мы всё равно залогируем время.
        status = "INTERNAL_ERROR"
        http_status = 500
        raise
    finally:
        duration_ms = (perf_counter() - start) * 1000.0
        entry0 = body.entries[0] if body.entries else None
        user_id = getattr(entry0, "user_id", None)
        store_id = getattr(entry0, "store_id", None)
        log_payload = {
            "ts": datetime.now(tz=timezone.utc).isoformat(),
            "level": "INFO",
            "event": "features_request",
            "brand": body.brand.value if body.brand else None,
            "user_id": user_id,
            "store_id": store_id,
            "items_count": len(body.items or []),
            "groups": groups,
            "duration_ms": round(duration_ms, 2),
            "http_status": http_status,
            "status": status,
            "response_cache_hit": response_cache_hit,
        }
        logger.info(json.dumps(log_payload, ensure_ascii=False))

    return result
