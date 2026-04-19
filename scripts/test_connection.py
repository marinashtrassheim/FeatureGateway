import asyncio
import sys
from pathlib import Path

import msgpack

# Добавляем корень проекта в PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.repositories.keydb_client import KeyDbClient
from app.repositories.v1.repository import TestRepositoryV1
from app.core.config import settings
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_connection():
    """Тестирует подключение к KeyDB и выполняет примеры запросов"""

    # Создаем клиенты для двух БД
    client_ds = KeyDbClient(settings.KEYDB_DS_URL)
    client_ds_second = KeyDbClient(settings.KEYDB_DS_SECOND_URL)

    try:
        # Подключаемся
        await client_ds.connect()
        await client_ds_second.connect()
        logger.info("✅ Successfully connected to both KeyDB instances")

        # Создаем репозиторий для тестов
        repo = TestRepositoryV1(client_ds, client_ds_second)

        # ТЕСТ 1: Получить город по store_id
        store_id = 12
        logger.info(f"\n📝 TEST 1: Getting city for store_id={store_id}")
        city = await repo.get_store_city(store_id)
        if city:
            logger.info(f"✅ Store {store_id} -> City {city}")
        else:
            logger.warning(f"⚠️ No city found for store_id={store_id}")

        # ТЕСТ 2: Получить города пользователя
        user_id = 95886331  # Пример user_id из твоего описания
        logger.info(f"\n📝 TEST 2: Getting cities for user_id={user_id}")
        cities = await repo.get_user_cities(user_id)
        if cities:
            logger.info(f"✅ User {user_id} -> Cities {cities}")
        else:
            logger.warning(f"⚠️ No cities found for user_id={user_id}")

        # ТЕСТ 3: Получить колонки для pers_item
        logger.info(f"\n📝 TEST 3: Getting columns for pers_item")
        columns = await repo.get_feature_columns("pers_item")
        if columns:
            logger.info(f"✅ pers_item columns: {columns}... (total: {len(columns)})")
        else:
            logger.warning(f"⚠️ No columns found for pers_item")

        # ТЕСТ 4: Получить колонки для pers_offl
        logger.info(f"\n📝 TEST 4: Getting columns for pers_offl")
        columns = await repo.get_feature_columns("pers_offl")
        if columns:
            logger.info(f"✅ pers_offl columns: {columns}... (total: {len(columns)})")
        else:
            logger.warning(f"⚠️ No columns found for pers_item")


        # ТЕСТ 5: Получить колонки для pers_user_item
        logger.info(f"\n📝 TEST 5: Getting columns for pers_user_item")
        columns = await repo.get_feature_columns("pers_user_item")
        if columns:
            logger.info(f"✅ pers_user_item columns: {columns}... (total: {len(columns)})")
        else:
            logger.warning(f"⚠️ No columns found for pers_user_item")

        # ТЕСТ 6: Получить признаки товара и распаковать их
        brand = "lo"
        city_id = 266
        item_id = 684866
        logger.info(f"\n📝 TEST 6: Getting item features for {brand}:{city_id}:{item_id}")
        features_raw = await repo.get_item_features(brand, city_id, item_id)
        if features_raw:
            # Десериализуем msgpack в список чисел
            try:
                features_list = msgpack.unpackb(features_raw, raw=False)
                logger.info(f"✅ Found features for item {item_id}")
                logger.info(f"   Распакованные значения: {features_list}")
                # Если есть колонки, можно показать с названиями
                columns = await repo.get_feature_columns("pers_item")
                if columns and len(columns) == len(features_list):
                    paired = dict(zip(columns, features_list))
                    logger.info(f"   С названиями: {paired}")
                else:
                    logger.info(f"   (колонки не загружены или не совпадают по длине)")
            except Exception as e:
                logger.warning(f"   Не удалось распаковать msgpack: {e}")
                logger.info(f"   Raw bytes: {features_raw[:50]}")
        else:
            logger.warning(f"⚠️ No features found for item {item_id}")

        # ТЕСТ 7: Получить признаки связки пользователь-товар и распаковать
        user_id = 100016192
        city_id = 84
        item_id = 287887
        logger.info(f"\n📝 TEST 7: Getting user-item features for {brand}:{user_id}:{city_id}:{item_id}")
        features_raw = await repo.get_user_item_features(brand, user_id, city_id, item_id)
        if features_raw:
            try:
                features_list = msgpack.unpackb(features_raw, raw=False)
                logger.info(f"✅ Found user-item features for item {item_id}")
                logger.info(f"   Распакованные значения: {features_list}")
                # Загружаем колонки для pers_user_item
                columns = await repo.get_feature_columns("pers_user_item")
                if columns and len(columns) == len(features_list):
                    paired = dict(zip(columns, features_list))
                    logger.info(f"   С названиями: {paired}")
                else:
                    logger.info(f"   (колонки не загружены или не совпадают по длине)")
            except Exception as e:
                logger.warning(f"   Не удалось распаковать msgpack: {e}")
                logger.info(f"   Raw bytes: {features_raw[:50]}")
        else:
            logger.warning(f"⚠️ No user-item features found for item {item_id}")

        # ТЕСТ 8: Получить офлайн признаки и распаковать
        user_id = 100000026
        item_id = 558630
        logger.info(f"\n📝 TEST 8: Getting offline features for user {user_id}, item {item_id}")
        features_raw = await repo.get_offline_features(user_id, item_id)
        if features_raw:
            try:
                features_list = msgpack.unpackb(features_raw, raw=False)
                logger.info(f"✅ Found offline features for user {user_id}, item {item_id}")
                logger.info(f"   Распакованные значения: {features_list}")
                # Загружаем колонки для pers_offl
                columns = await repo.get_feature_columns("pers_offl")
                if columns and len(columns) == len(features_list):
                    paired = dict(zip(columns, features_list))
                    logger.info(f"   С названиями: {paired}")
                else:
                    logger.info(f"   (колонки не загружены или не совпадают по длине)")
            except Exception as e:
                logger.warning(f"   Не удалось распаковать msgpack: {e}")
                logger.info(f"   Raw bytes: {features_raw[:50]}")
        else:
            logger.warning(f"⚠️ No offline features found for user {user_id}, item {item_id}")


        logger.info("\n" + "=" * 50)
        logger.info("✅ All tests completed!")

    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise
    finally:
        # Закрываем соединения
        await client_ds.disconnect()
        await client_ds_second.disconnect()


if __name__ == "__main__":
    asyncio.run(test_connection())