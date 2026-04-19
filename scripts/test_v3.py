import asyncio
import sys
from pathlib import Path
import msgpack
import logging

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.repositories.keydb_client import KeyDbClient
from app.repositories.v3.repository import TestRepositoryV3
from app.core.config import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_connection():
    client_ds = KeyDbClient(settings.KEYDB_V3_URL)  # порт 6381
    try:
        await client_ds.connect()
        logger.info("✅ Connected to v3 (msgpack dict)")
        repo = TestRepositoryV3(client_ds)

        # ТЕСТ 1-5 такие же, как в v1 (методы репозитория работают одинаково)
        store_id = 12
        city = await repo.get_store_city(store_id)
        logger.info(f"Store {store_id} -> City {city}")

        user_id = 105977884
        cities = await repo.get_user_cities(user_id)
        logger.info(f"User {user_id} -> Cities {cities}")

        cols = await repo.get_feature_columns("pers_item")
        logger.info(f"pers_item columns: {cols[:5]}... total {len(cols)}")

        cols = await repo.get_feature_columns("pers_offl")
        logger.info(f"pers_offl columns: {cols}")

        cols = await repo.get_feature_columns("pers_user_item")
        logger.info(f"pers_user_item columns: {cols[:5]}... total {len(cols)}")

        # ТЕСТ 6: v3 хранит словарь, выводим его напрямую
        brand = "lo"; city_id = 266; item_id = 655642
        raw = await repo.get_item_features(brand, city_id, item_id)
        if raw:
            data = msgpack.unpackb(raw, raw=False)
            logger.info(f"Item {item_id} features (dict): {data}")
            # Для v3 не пытаемся сопоставлять с колонками, так как ключи уже есть в словаре
        else:
            logger.warning("No data")

        # ТЕСТ 7
        user_id = 100021532; city_id = 70; item_id = 275462
        raw = await repo.get_user_item_features(brand, user_id, city_id, item_id)
        if raw:
            data = msgpack.unpackb(raw, raw=False)
            logger.info(f"User-item features (dict): {data}")

        # ТЕСТ 8
        user_id = 100021532; item_id = 135599
        raw = await repo.get_offline_features(user_id, item_id)
        if raw:
            data = msgpack.unpackb(raw, raw=False)
            logger.info(f"Offline features (list): {data}")  # pers_offl в v3 тоже список, не словарь
            # для pers_offl можно показать с колонками, если нужно
            if cols and len(cols) == len(data):
                logger.info(f"With names: {dict(zip(cols, data))}")

        logger.info("✅ All tests completed for v3")
    finally:
        await client_ds.disconnect()

if __name__ == "__main__":
    asyncio.run(test_connection())