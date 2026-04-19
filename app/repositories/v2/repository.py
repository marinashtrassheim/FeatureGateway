import json
from typing import Any
import logging
import msgpack
from app.repositories.keydb_client import KeyDbClient

logger = logging.getLogger(__name__)

class V2FeatureRepository:
    """Хранилище v2: JSON-строки в значениях."""

    def __init__(self, keydb_ds: KeyDbClient, keydb_ds_second: KeyDbClient | None = None):
        self.ds = keydb_ds
        self.ds_second = keydb_ds_second or keydb_ds

    async def get_store_city(self, store_id: int) -> int | None:
        key = f"pers_hub_city:{store_id}"
        try:
            value = await self.ds.get(key)
            if value:
                decoded = value.decode("utf-8")
                try:
                    return int(json.loads(decoded))
                except json.JSONDecodeError:
                    return int(decoded)
            return None
        except Exception as e:
            logger.error(
                "keydb_error %s",
                {
                    "event": "keydb_error",
                    "storage_version": "v2",
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
                cities_str = value.decode("utf-8")
                cities_list = json.loads(cities_str)
                if not isinstance(cities_list, list):
                    cities_list = [cities_list]
                return [int(city) for city in cities_list]
            return []
        except Exception as e:
            logger.error(
                "keydb_error %s",
                {
                    "event": "keydb_error",
                    "storage_version": "v2",
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
                cols = json.loads(v.decode("utf-8"))
                if not isinstance(cols, list):
                    cols = [cols]
                out[key] = [str(c) for c in cols]
            return out
        except Exception as e:
            logger.error(
                "keydb_error %s",
                {
                    "event": "keydb_error",
                    "storage_version": "v2",
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
            return self._decode_hash_list(raw)
        except Exception as e:
            logger.error(
                "keydb_error %s",
                {
                    "event": "keydb_error",
                    "storage_version": "v2",
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
            for item, raw in zip(items, raw_rows):
                if not raw:
                    continue
                out[item] = json.loads(raw.decode("utf-8"))
            return out
        except Exception as e:
            logger.error(
                "keydb_error %s",
                {
                    "event": "keydb_error",
                    "storage_version": "v2",
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
            return self._decode_hash_list(raw)
        except Exception as e:
            logger.error(
                "keydb_error %s",
                {
                    "event": "keydb_error",
                    "storage_version": "v2",
                    "operation": "get_pers_offl",
                    "key": key,
                    "exception": str(e),
                },
            )
            return {}

    def _decode_hash_list(self, raw: dict[Any, Any]) -> dict[int, list[Any]]:
        out: dict[int, list[Any]] = {}
        for k, v in raw.items():
            item = int(k.decode("utf-8") if isinstance(k, bytes) else str(k))
            out[item] = self._decode_feature_row(v)
        return out

    @staticmethod
    def _decode_feature_row(raw: Any) -> list[Any]:
        """
        V2 может содержать смешанный формат по значениям:
        - JSON-строка (например "[37, 61]")
        - msgpack bytes (например с префиксом 0x92)
        """
        if isinstance(raw, bytes):
            try:
                return json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return msgpack.unpackb(raw, raw=False)
        if isinstance(raw, str):
            return json.loads(raw)
        # fallback для неожиданных случаев
        return list(raw)

    # Backward-compatible helpers for existing scripts.
    async def get_feature_columns(self, feature_type: str) -> list[str] | None:
        return (await self.get_pers_cols()).get(feature_type)

    async def get_item_features(self, brand: str, city_id: int, item_id: int) -> bytes | None:
        row = (await self.get_pers_item_by_items(brand, city_id, [item_id])).get(item_id)
        return None if row is None else json.dumps(row).encode("utf-8")

    async def get_user_item_features(
        self, brand: str, user_id: int, city_id: int, item_id: int
    ) -> bytes | None:
        row = (await self.get_pers_user_item(brand, user_id, city_id)).get(item_id)
        return None if row is None else json.dumps(row).encode("utf-8")

    async def get_offline_features(self, user_id: int, item_id: int) -> bytes | None:
        row = (await self.get_pers_offl(user_id)).get(item_id)
        return None if row is None else json.dumps(row).encode("utf-8")


TestRepositoryV2 = V2FeatureRepository