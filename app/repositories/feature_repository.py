import json
import logging
from typing import Any

from app.core.exceptions import (
    FeatureStorageDataFormatError,
    FeatureStorageUnavailableError,
)
from app.repositories.keydb_client import KeyDbClient

logger = logging.getLogger(__name__)


class KeyDbFeatureRepository:
    """Доступ к признакам в KeyDB"""

    def __init__(self, keydb_ds: KeyDbClient, keydb_ds_second: KeyDbClient | None = None):
        self.ds = keydb_ds
        self.ds_second = keydb_ds_second or keydb_ds

    @staticmethod
    def _raise_keydb_unavailable(operation: str, key: str, error: Exception) -> None:
        logger.error(
            "keydb_error %s",
            {
                "event": "keydb_error",
                "operation": operation,
                "key": key,
                "exception": str(error),
            },
        )
        raise FeatureStorageUnavailableError(
            f"KeyDB unavailable during {operation}",
            operation=operation,
            key=key,
        ) from error

    @staticmethod
    def _raise_keydb_data_format(operation: str, key: str, error: Exception) -> None:
        logger.error(
            "keydb_data_format_error %s",
            {
                "event": "keydb_data_format_error",
                "operation": operation,
                "key": key,
                "exception": str(error),
            },
        )
        raise FeatureStorageDataFormatError(
            f"Invalid KeyDB payload during {operation}",
            operation=operation,
            key=key,
        ) from error

    async def get_store_city(self, store_id: int) -> int | None:
        key = f"pers_hub_city:{store_id}"
        operation = "get_store_city"
        try:
            value = await self.ds.get(key)
            if not value:
                return None
            decoded = value.decode("utf-8")
            try:
                return int(json.loads(decoded))
            except json.JSONDecodeError:
                return int(decoded)
        except Exception as e:
            self._raise_keydb_unavailable(operation, key, e)

    async def get_user_cities(self, user_id: int) -> list[int]:
        key = "pers_user_city"
        operation = "get_user_cities"
        try:
            value = await self.ds.hget(key, str(user_id))
            if not value:
                return []
            cities_str = value.decode("utf-8")
            cities_list = json.loads(cities_str)
            if not isinstance(cities_list, list):
                cities_list = [cities_list]
            return [int(city) for city in cities_list]
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            self._raise_keydb_data_format(operation, f"{key}:{user_id}", e)
        except Exception as e:
            self._raise_keydb_unavailable(operation, key, e)

    def _decode_hub_city_bytes(self, value: bytes | None) -> int | None:
        if not value:
            return None
        decoded = value.decode("utf-8")
        try:
            return int(json.loads(decoded))
        except json.JSONDecodeError:
            return int(decoded)

    def _decode_user_cities_bytes(self, value: bytes | None) -> list[int]:
        if not value:
            return []
        cities_str = value.decode("utf-8")
        cities_list = json.loads(cities_str)
        if not isinstance(cities_list, list):
            cities_list = [cities_list]
        return [int(city) for city in cities_list]

    async def resolve_city_context(
        self, store_id: int | None, user_id: int | None
    ) -> tuple[int, list[int]]:
        """
        Определение города: один pipeline GET pers_hub_city + HGET pers_user_city,
        если есть и ТЦ, и пользователь; иначе минимальное число команд.
        """
        has_store = store_id is not None and store_id != -1
        has_user = user_id is not None

        if has_store and has_user:
            hub_key = f"pers_hub_city:{store_id}"
            operation = "resolve_city_context"
            try:
                pipe = self.ds.pipeline()
                pipe.get(hub_key)
                pipe.hget("pers_user_city", str(user_id))
                raw_store, raw_user = await pipe.execute()
            except Exception as e:
                self._raise_keydb_unavailable(operation, hub_key, e)
            city = self._decode_hub_city_bytes(
                raw_store if isinstance(raw_store, bytes) else None
            )
            if city is not None:
                return city, []
            try:
                user_cities = self._decode_user_cities_bytes(
                    raw_user if isinstance(raw_user, bytes) else None
                )
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                self._raise_keydb_data_format(operation, f"pers_user_city:{user_id}", e)
            return -1, user_cities

        if has_store and not has_user:
            c = await self.get_store_city(store_id)
            return (c if c is not None else -1), []

        if not has_store and has_user:
            assert user_id is not None
            return -1, await self.get_user_cities(user_id)

        return -1, []

    async def get_pers_cols(self) -> dict[str, list[str]]:
        key = "pers_cols"
        operation = "get_pers_cols"
        try:
            raw = await self.ds.hgetall(key)
            out: dict[str, list[str]] = {}
            for k, v in raw.items():
                group = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                cols = json.loads(v.decode("utf-8"))
                if not isinstance(cols, list):
                    cols = [cols]
                out[group] = [str(c) for c in cols]
            return out
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            self._raise_keydb_data_format(operation, key, e)
        except Exception as e:
            self._raise_keydb_unavailable(operation, key, e)

    async def get_pers_user_item(
        self,
        brand: str,
        user_id: int,
        city_id: int,
    ) -> dict[int, list[Any]]:
        key = f"pers_user_item:{brand}:{user_id}:{city_id}"
        operation = "get_pers_user_item"
        try:
            raw = await self.ds.hgetall(key)
            return self._decode_hash_list(raw)
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            self._raise_keydb_data_format(operation, key, e)
        except Exception as e:
            self._raise_keydb_unavailable(operation, key, e)

    async def get_pers_item_by_items(
        self,
        brand: str,
        city_id: int,
        items: list[int],
    ) -> dict[int, list[Any]]:
        key = f"pers_item:{brand}:{city_id}"
        operation = "get_pers_item_by_items"
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
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            self._raise_keydb_data_format(operation, key, e)
        except Exception as e:
            self._raise_keydb_unavailable(operation, key, e)

    async def get_pers_offl(self, user_id: int) -> dict[int, list[Any]]:
        key = f"pers_offl:{user_id}"
        operation = "get_pers_offl"
        try:
            raw = await self.ds_second.hgetall(key)
            return self._decode_hash_list(raw)
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            self._raise_keydb_data_format(operation, key, e)
        except Exception as e:
            self._raise_keydb_unavailable(operation, key, e)

    def _decode_hash_list(self, raw: dict[Any, Any]) -> dict[int, list[Any]]:
        out: dict[int, list[Any]] = {}
        for k, v in raw.items():
            item = int(k.decode("utf-8") if isinstance(k, bytes) else str(k))
            out[item] = self._decode_feature_row(v)
        return out

    @staticmethod
    def _decode_feature_row(raw: Any) -> list[Any]:
        """
        Значения в hash должны быть JSON-массивом (bytes/str).
        """
        value: Any
        if isinstance(raw, bytes):
            value = json.loads(raw.decode("utf-8"))
        elif isinstance(raw, str):
            value = json.loads(raw)
        else:
            raise ValueError("Unsupported feature row payload type")

        if not isinstance(value, list):
            raise ValueError("Feature row must be a JSON array")
        return value

    async def fetch_primary_pui_pi_bundle(
        self,
        *,
        brand: str,
        user_id: int | None,
        pers_item_city_id: int,
        items: list[int],
        pui_city_ids: tuple[int, ...],
    ) -> tuple[list[dict[int, list[Any]]], dict[int, list[Any]]]:
        """
        Один pipeline на основном KeyDB: все pers_user_item по городам,
        затем все HGET по pers_item для списка товаров (как common_rank_wrapper).
        """
        if not pui_city_ids and not items:
            return [], {}

        n_pui = len(pui_city_ids)
        pi_key = f"pers_item:{brand}:{pers_item_city_id}"
        operation = "fetch_primary_pui_pi_bundle"
        try:
            pipe = self.ds.pipeline()
            for cid in pui_city_ids:
                assert user_id is not None
                pipe.hgetall(f"pers_user_item:{brand}:{user_id}:{cid}")
            for item in items:
                pipe.hget(pi_key, str(item))
            raw_all = await pipe.execute()
        except Exception as e:
            self._raise_keydb_unavailable(operation, pi_key, e)

        pui_raws = raw_all[:n_pui]
        pi_raws = raw_all[n_pui:]

        pui_out: list[dict[int, list[Any]]] = []
        for raw in pui_raws:
            if not isinstance(raw, dict):
                pui_out.append({})
                continue
            try:
                pui_out.append(self._decode_hash_list(raw))
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                self._raise_keydb_data_format(operation, "pers_user_item", e)

        pi_out: dict[int, list[Any]] = {}
        for item, raw in zip(items, pi_raws):
            if not raw:
                continue
            try:
                pi_out[item] = json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                self._raise_keydb_data_format(operation, pi_key, e)
        return pui_out, pi_out

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
