import asyncio
import sys
from pathlib import Path
import json
import logging

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.repositories.keydb_client import KeyDbClient
from app.repositories.v2.repository import TestRepositoryV2
from app.core.config import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_connection():
    client_ds = KeyDbClient(settings.KEYDB_V2_URL)  # порт 6380
    try:
        await client_ds.connect()
        logger.info("✅ Connected to v2 (JSON)")
        repo = TestRepositoryV2(client_ds)

        # ТЕСТ 1-5 аналогичны v1, но методы репозитория уже возвращают нужные типы
        store_id = 12
        city = await repo.get_store_city(store_id)
        logger.info(f"Ключ pers_hub_city Store {store_id} -> City {city}")

        user_id = 105977884
        cities = await repo.get_user_cities(user_id)
        logger.info(f"Ключ pers_user_city User {user_id} -> Cities {cities}")

        cols = await repo.get_feature_columns("pers_item")
        logger.info(f"Ключ pers_colls -pers_item columns: {cols}... total {len(cols)}")

        cols = await repo.get_feature_columns("pers_offl")
        logger.info(f"Ключ pers_colls -pers_offl columns: {cols}")

        cols = await repo.get_feature_columns("pers_user_item")
        logger.info(f"Ключ pers_colls -pers_user_item columns: {cols}... total {len(cols)}")

        # ТЕСТ 6: JSON-строка → распарсить через json
        brand = "lo"; city_id = 266; item_id = 655642
        raw = await repo.get_item_features(brand, city_id, item_id)
        if raw:
            data = json.loads(raw.decode('utf-8'))
            logger.info(f"Ключ pers_item - Item {item_id} features (JSON list): {data[:5]}...")
            if cols and len(cols) == len(data):
                logger.info(f"Ключ pers_item - With names: {dict(zip(cols, data))}")

        # ТЕСТ 7
        user_id = 100021532; city_id = 70; item_id = 275462
        raw = await repo.get_user_item_features(brand, user_id, city_id, item_id)
        if raw:
            data = json.loads(raw.decode('utf-8'))
            logger.info(f"Ключ pers_user_item - User-item features (JSON list): {data[:5]}...")
            cols = await repo.get_feature_columns("pers_user_item")
            if cols and len(cols) == len(data):
                logger.info(f"With names: {dict(zip(cols, data))}")

        # ТЕСТ 8
        user_id = 100021532; item_id = 135599
        raw = await repo.get_offline_features(user_id, item_id)
        if raw:
            data = json.loads(raw.decode('utf-8'))
            logger.info(f"Ключ pers_offl -Offline features (JSON list): {data}")
            cols = await repo.get_feature_columns("pers_offl")
            if cols and len(cols) == len(data):
                logger.info(f"Ключ pers_offl - With names: {dict(zip(cols, data))}")

        logger.info("✅ All tests completed for v2")
    finally:
        await client_ds.disconnect()

if __name__ == "__main__":
    asyncio.run(test_connection())