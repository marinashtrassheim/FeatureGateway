from __future__ import annotations

import msgpack
from typing import Any
import logging
from app.core.feature_config import PERS_ITEM_MULTIPLIERS, PERS_USER_ITEM_MULTIPLIERS
from app.repositories.keydb_client import KeyDbClient

logger = logging.getLogger(__name__)

class V1FeatureRepository:
    """Хранилище v1: msgpack в значениях hash."""

    def __init__(self, keydb_ds: KeyDbClient, keydb_ds_second: KeyDbClient | None = None):
        self.ds = keydb_ds
        self.ds_second = keydb_ds_second or keydb_ds

    async def get_store_city(self, store_id: int) -> int | None:
        key = f"pers_hub_city:{store_id}"
        try:
            value = await self.ds.get(key)
            if value:
                unpacked = msgpack.unpackb(value, raw=False)
                return int(unpacked)
            return None
        except Exception as e:
            logger.error(
                "keydb_error %s",
                {
                    "event": "keydb_error",
                    "storage_version": "v1",
                    "operation": "get_store_city",
                    "key": key,
                    "exception": str(e),
                },
            )
            return None

    async def get_user_cities(self, user_id: int) -> list[int]:
        key = "pers_user_city"
        try:
            value = await self.ds.hget(key, str(user_id))
            if value:
                cities_list = msgpack.unpackb(value, raw=False)
                if not isinstance(cities_list, list):
                    cities_list = [cities_list]
                return [int(city) for city in cities_list]
            return []
        except Exception as e:
            logger.error(
                "keydb_error %s",
                {
                    "event": "keydb_error",
                    "storage_version": "v1",
                    "operation": "get_user_cities",
                    "key": key,
                    "exception": str(e),
                },
            )
            return []

    async def get_pers_cols(self) -> dict[str, list[str]]:
        try:
            raw = await self.ds.hgetall("pers_cols")
            out: dict[str, list[str]] = {}
            for k, v in raw.items():
                key = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                cols = msgpack.unpackb(v, raw=False)
                if not isinstance(cols, list):
                    cols = [cols]
                out[key] = [str(c) for c in cols]
            return out
        except Exception as e:
            logger.error(
                "keydb_error %s",
                {
                    "event": "keydb_error",
                    "storage_version": "v1",
                    "operation": "get_pers_cols",
                    "key": "pers_cols",
                    "exception": str(e),
                },
            )
            return {}

    async def get_pers_user_item(
        self,
        brand: str,
        user_id: int,
        city_id: int,
        *,
        columns: list[str] | None = None,
    ) -> dict[int, list[Any]]:
        key = f"pers_user_item:{brand}:{user_id}:{city_id}"
        try:
            raw = await self.ds.hgetall(key)
            cols = (
                columns if columns is not None else await self._get_cols("pers_user_item")
            )
            return self._decode_hash_list(raw, cols, PERS_USER_ITEM_MULTIPLIERS)
        except Exception as e:
            logger.error(
                "keydb_error %s",
                {
                    "event": "keydb_error",
                    "storage_version": "v1",
                    "operation": "get_pers_user_item",
                    "key": key,
                    "exception": str(e),
                },
            )
            return {}

    async def get_pers_item_by_items(
        self,
        brand: str,
        city_id: int,
        items: list[int],
        *,
        columns: list[str] | None = None,
    ) -> dict[int, list[Any]]:
        key = f"pers_item:{brand}:{city_id}"
        try:
            pipe = self.ds.pipeline()
            for item in items:
                pipe.hget(key, str(item))
            raw_rows = await pipe.execute()
            out: dict[int, list[Any]] = {}
            cols = columns if columns is not None else await self._get_cols("pers_item")
            for item, raw in zip(items, raw_rows):
                if not raw:
                    continue
                row = msgpack.unpackb(raw, raw=False)
                out[item] = self._decode_row(row, cols, PERS_ITEM_MULTIPLIERS)
            return out
        except Exception as e:
            logger.error(
                "keydb_error %s",
                {
                    "event": "keydb_error",
                    "storage_version": "v1",
                    "operation": "get_pers_item_by_items",
                    "key": key,
                    "exception": str(e),
                },
            )
            return {}

    async def get_pers_offl(self, user_id: int) -> dict[int, list[Any]]:
        key = f"pers_offl:{user_id}"
        try:
            raw = await self.ds_second.hgetall(key)
            return self._decode_hash_list(raw, None, None)
        except Exception as e:
            logger.error(
                "keydb_error %s",
                {
                    "event": "keydb_error",
                    "storage_version": "v1",
                    "operation": "get_pers_offl",
                    "key": key,
                    "exception": str(e),
                },
            )
            return {}

    async def _get_cols(self, feature_type: str) -> list[str]:
        value = await self.ds.hget("pers_cols", feature_type)
        if not value:
            return []
        cols = msgpack.unpackb(value, raw=False)
        if not isinstance(cols, list):
            cols = [cols]
        return [str(c) for c in cols]

    def _decode_hash_list(
        self,
        raw: dict[Any, Any],
        cols: list[str] | None,
        multipliers: dict[str, int] | None,
    ) -> dict[int, list[Any]]:
        out: dict[int, list[Any]] = {}
        for k, v in raw.items():
            item = int(k.decode("utf-8") if isinstance(k, bytes) else str(k))
            row = msgpack.unpackb(v, raw=False)
            out[item] = self._decode_row(row, cols, multipliers)
        return out

    @staticmethod
    def _decode_row(
        row: Any,
        cols: list[str] | None,
        multipliers: dict[str, int] | None,
    ) -> list[Any]:
        if isinstance(row, dict):
            if not cols:
                return []
            out: list[Any] = []
            for name in cols:
                raw_value = row.get(name, 0)
                out.append(V1FeatureRepository._restore_scaled_value(name, raw_value, multipliers))
            return out
        if not isinstance(row, list):
            return list(row) if isinstance(row, tuple) else [row]
        if not cols or not multipliers:
            return row
        out: list[Any] = []
        for i, v in enumerate(row):
            if i >= len(cols):
                out.append(v)
                continue
            out.append(V1FeatureRepository._restore_scaled_value(cols[i], v, multipliers))
        return out

    @staticmethod
    def _restore_scaled_value(
        feature_name: str, raw_value: Any, multipliers: dict[str, int] | None
    ) -> Any:
        if not multipliers:
            return raw_value
        mul = multipliers.get(feature_name, 1)
        if mul == 1:
            return raw_value
        if not isinstance(raw_value, (int, float)):
            return raw_value
        value = raw_value / mul
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return value

    # Backward-compatible helpers for existing scripts.
    async def get_feature_columns(self, feature_type: str) -> list[str] | None:
        return (await self.get_pers_cols()).get(feature_type)

    async def get_item_features(self, brand: str, city_id: int, item_id: int) -> bytes | None:
        row = (await self.get_pers_item_by_items(brand, city_id, [item_id])).get(item_id)
        return None if row is None else msgpack.packb(row, use_bin_type=True)

    async def get_user_item_features(
        self, brand: str, user_id: int, city_id: int, item_id: int
    ) -> bytes | None:
        row = (await self.get_pers_user_item(brand, user_id, city_id)).get(item_id)
        return None if row is None else msgpack.packb(row, use_bin_type=True)

    async def get_offline_features(self, user_id: int, item_id: int) -> bytes | None:
        row = (await self.get_pers_offl(user_id)).get(item_id)
        return None if row is None else msgpack.packb(row, use_bin_type=True)


TestRepositoryV1 = V1FeatureRepository