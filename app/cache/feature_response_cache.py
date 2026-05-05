"""Redis cache-aside для ответа POST /features (отдельный инстанс от KeyDB)."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

import redis.asyncio as redis

from app.api.v1.schemas.request import FeatureRequest
from app.api.v1.schemas.response import FeatureResponse
from app.core.config import settings

logger = logging.getLogger(__name__)

# Версия feature-контракта, влияющего на итоговую семантику ответа.
# При изменении логики агрегации/registry/контракта ответа увеличивайте версию
# (например, v1 -> v2), чтобы старые cache-key перестали совпадать.
FEATURE_CONTRACT_VERSION = "v1"


def _normalize_requested_features(rf: Any) -> dict[str, Any]:
    def norm(xs: list[str] | None) -> list[str] | None:
        if xs is None:
            return None
        return sorted(xs)

    return {
        "pers_user_item": norm(rf.pers_user_item),
        "pers_item": norm(rf.pers_item),
        "pers_offl": norm(rf.pers_offl),
    }


def _cache_key_payload(body: FeatureRequest) -> dict[str, Any]:
    """Только поля, влияющие на результат оркестрации."""
    # Текущий контракт обрабатывает ровно один контекст (entries[0]).
    e0 = body.entries[0] if body.entries else None
    entry_payload = {
        "user_id": e0.user_id if e0 else None,
        "store_id": e0.store_id if e0 else None,
        "search_query_norm": e0.search_query_norm if e0 else None,
        "channel": e0.channel.value if (e0 and e0.channel is not None) else None,
    }
    return {
        "contract_version": FEATURE_CONTRACT_VERSION,
        "brand": body.brand.value if body.brand else None,
        "items": sorted(body.items or []),
        "entry": entry_payload,
        "requested_features": _normalize_requested_features(body.requested_features),
    }


def build_response_cache_key(body: FeatureRequest) -> str:
    raw = json.dumps(_cache_key_payload(body), sort_keys=True, ensure_ascii=False)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"gw:features:{FEATURE_CONTRACT_VERSION}:{digest}"


class FeatureResponseCache:
    def __init__(self, url: str | None, ttl_seconds: int) -> None:
        self._url = (url or "").strip() or None
        self._ttl = ttl_seconds
        self._client: redis.Redis | None = None
        # Если Redis недоступен при старте, не падаем целиком — шлюз без кэша ответа.
        self._connect_failed = False

    @property
    def enabled(self) -> bool:
        return self._url is not None and not self._connect_failed

    async def connect(self) -> None:
        if not self._url:
            return
        try:
            self._client = redis.from_url(
                self._url,
                decode_responses=False,
                socket_connect_timeout=settings.KEYDB_CONNECT_TIMEOUT,
                socket_timeout=settings.KEYDB_READ_TIMEOUT,
                max_connections=settings.KEYDB_MAX_CONNECTIONS,
            )
            await self._client.ping()
            logger.info("Connected to feature response cache at %s", self._url)
        except Exception as e:
            self._connect_failed = True
            self._client = None
            logger.warning(
                "Feature response cache недоступен (%s), работаем без кэша ответа: %s",
                self._url,
                e,
            )

    async def disconnect(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None
            logger.info("Disconnected from feature response cache")

    async def get(self, body: FeatureRequest) -> FeatureResponse | None:
        if not self._client:
            return None
        key = build_response_cache_key(body)
        try:
            raw = await self._client.get(key)
        except Exception as e:
            logger.warning("response cache GET failed: %s", e)
            return None
        if not raw:
            return None
        try:
            return FeatureResponse.model_validate_json(raw.decode("utf-8"))
        except Exception as e:
            logger.warning("response cache decode failed, ignoring: %s", e)
            return None

    async def set(self, body: FeatureRequest, response: FeatureResponse) -> None:
        if not self._client:
            return
        key = build_response_cache_key(body)
        try:
            payload = response.model_dump_json().encode("utf-8")
            await self._client.set(key, payload, ex=self._ttl)
        except Exception as e:
            logger.warning("response cache SET failed: %s", e)
