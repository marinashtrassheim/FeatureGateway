import asyncio
import sys
from pathlib import Path
import msgpack
import logging
from app.repositories.keydb_client import KeyDbClient
from app.repositories.v1.repository import TestRepositoryV1
from app.core.config import settings

sys.path.insert(0, str(Path(__file__).parent.parent))



logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_connection():
    client_ds = KeyDbClient(settings.KEYDB_V1_URL)  # должен быть определён в config
    try:
        await client_ds.connect()
        logger.info("✅ Connected to v1 (msgpack list)")
        repo = TestRepositoryV1(client_ds)

        # ТЕСТ 1
        store_id = 12
        city = await repo.get_store_city(store_id)
        logger.info(f"✅ Ключ pers_hub_city Store {store_id} -> City {city}")

        # ТЕСТ 2
        user_id = 105977884
        cities = await repo.get_user_cities(user_id)
        logger.info(f"✅ Ключ pers_user_city  User {user_id} -> Cities {cities}")

        # ТЕСТ 3
        cols = await repo.get_feature_columns("pers_item")
        logger.info(f"Ключ pers_colls - pers_item columns: {cols}... total {len(cols)}")

        # ТЕСТ 4
        cols = await repo.get_feature_columns("pers_offl")
        logger.info(f"Ключ pers_colls - pers_offl columns: {cols}")

        # ТЕСТ 5
        cols = await repo.get_feature_columns("pers_user_item")
        logger.info(f"Ключ pers_colls - pers_user_item columns: {cols}... total {len(cols)}")

        # ТЕСТ 6
        brand = "lo"; city_id = 266; item_id = 655642
        raw = await repo.get_item_features(brand, city_id, item_id)
        if raw:
            data = msgpack.unpackb(raw, raw=False)
            logger.info(f"Ключ pers_item - Item {item_id} features (list): {data}")
            if cols and len(cols) == len(data):
                logger.info(f"With names: {dict(zip(cols, data))}")

        # ТЕСТ 7
        user_id = 100021532; city_id = 70; item_id = 275462
        raw = await repo.get_user_item_features(brand, user_id, city_id, item_id)
        if raw:
            data = msgpack.unpackb(raw, raw=False)
            logger.info(f"Ключ pers_user_item - User-item features (list): {data}...")
            cols = await repo.get_feature_columns("pers_user_item")
            if cols and len(cols) == len(data):
                logger.info(f"With names: {dict(zip(cols, data))}")

        # ТЕСТ 8
        user_id = 100021532; item_id = 135599
        raw = await repo.get_offline_features(user_id, item_id)
        if raw:
            data = msgpack.unpackb(raw, raw=False)
            logger.info(f" Offline features (list): {data}")
            cols = await repo.get_feature_columns("pers_offl")
            if cols and len(cols) == len(data):
                logger.info(f"Ключ pers_offl - With names: {dict(zip(cols, data))}")

        logger.info("✅ All tests completed for v1")
    finally:
        await client_ds.disconnect()

if __name__ == "__main__":
    asyncio.run(test_connection())