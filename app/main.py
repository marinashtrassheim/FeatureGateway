from __future__ import annotations

from contextlib import asynccontextmanager
import json
import logging
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from starlette.responses import JSONResponse

from app.api.v1.endpoints.features import router as features_router
from app.core.config import get_keydb_url_by_version, settings
from app.core.exceptions import FeatureValidationError
from app.repositories.keydb_client import KeyDbClient
from app.cache.feature_response_cache import FeatureResponseCache


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Два клиента: KEYDB_DS_URL (как db_conn_1 в модели) и KEYDB_DS_SECOND_URL (pers_offl)."""
    ds = KeyDbClient(get_keydb_url_by_version(settings.STORAGE_VERSION))
    ds_second = KeyDbClient(settings.KEYDB_DS_SECOND_URL)
    await ds.connect()
    await ds_second.connect()
    app.state.keydb_ds = ds
    app.state.keydb_ds_second = ds_second

    response_cache = FeatureResponseCache(
        settings.FEATURE_RESPONSE_CACHE_URL,
        settings.FEATURE_RESPONSE_CACHE_TTL_SECONDS,
    )
    app.state.feature_response_cache = response_cache
    if response_cache.enabled:
        await response_cache.connect()

    yield

    if response_cache.enabled:
        await response_cache.disconnect()
    await ds.disconnect()
    await ds_second.disconnect()


logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",  # пишем уже готовые JSON-строки
)


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


app = FastAPI(
    title="Feature Gateway",
    version="0.1.0",
    description=(
        "API-шлюз между ML-модулями и KeyDB.\n\n"
        "Основной метод: `POST /api/v1/features` — универсальный запрос признаков "
        "`pers_item`, `pers_user_item`, `pers_offl`."
    ),
    lifespan=lifespan,
)

app.include_router(features_router, prefix="/api/v1")


@app.exception_handler(FeatureValidationError)
async def feature_validation_handler(
    request: Request,
    exc: FeatureValidationError,
) -> JSONResponse:
    logging.getLogger("feature_gateway").info(
        json.dumps(
            {
                "ts": _now_iso(),
                "level": "ERROR",
                "event": "features_request",
                "storage_version": settings.STORAGE_VERSION,
                "path": str(request.url.path),
                "http_status": 422,
                "status": "VALIDATION_ERROR",
                "error_code": exc.code,
                "error_message": exc.message,
            },
            ensure_ascii=False,
        )
    )
    return JSONResponse(
        status_code=422,
        content={
            "code": exc.code,
            "message": exc.message,
            "errors": exc.errors_as_dicts(),
        },
    )


@app.exception_handler(RequestValidationError)
async def pydantic_validation_handler(
    request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    logging.getLogger("feature_gateway").info(
        json.dumps(
            {
                "ts": _now_iso(),
                "level": "ERROR",
                "event": "features_request",
                "storage_version": settings.STORAGE_VERSION,
                "path": str(request.url.path),
                "http_status": 422,
                "status": "INVALID_BODY",
                "error_code": "VALIDATION_ERROR",
                "error_message": "Ошибка формата запроса",
            },
            ensure_ascii=False,
        )
    )
    return JSONResponse(
        status_code=422,
        content={
            "code": "VALIDATION_ERROR",
            "message": "Ошибка формата запроса",
            "errors": exc.errors(),
        },
    )
