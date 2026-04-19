"""In-process кэш для pers_cols (обновление ~раз в сутки)."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.repositories.base import FeatureRepository


class PersColsCache:
    def __init__(self, ttl_seconds: int) -> None:
        self._ttl = ttl_seconds
        self._lock = asyncio.Lock()
        self._data: dict[str, list[str]] | None = None
        self._expires_at: float = 0.0

    async def get(self, repo: FeatureRepository) -> dict[str, list[str]]:
        async with self._lock:
            now = time.monotonic()
            if self._data is not None and now < self._expires_at:
                return self._data
            self._data = await repo.get_pers_cols()
            self._expires_at = now + self._ttl
            return self._data


def build_pers_cols_cache(ttl_seconds: int) -> PersColsCache:
    return PersColsCache(ttl_seconds=ttl_seconds)
