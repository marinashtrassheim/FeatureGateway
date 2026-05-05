# Feature Gateway

API-шлюз между ML-модулями и Redis/KeyDB для групп признаков `pers_item`, `pers_user_item`, `pers_offl`.

Хранилище: JSON-строки в значениях hash.

## Локальный запуск

### Зависимости

- Python 3.10+
- Docker с Compose v2

### Клонирование и окружение

```bash
git clone <url> FeatureGateway
cd FeatureGateway
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Redis

Поднимаются **два** контейнера: фичи и отдельный кэш ответа API.

```bash
docker compose up -d
```

- **Фичи** (и `pers_offl` локально на том же инстансе): **`redis://127.0.0.1:6379/0`**
- **Кэш ответа** `POST /features`: **`redis://127.0.0.1:6382/0`**

Данные в compose хранятся в **именованных volumes** (`redis-data`, `redis-cache-data`), чтобы после `docker compose down` ключи не исчезали. Раньше без volume данные жили только в слое контейнера: при пересоздании образа/контейнера или `docker compose down -v` Redis мог оказаться **пустым** — это не адрес `127.0.0.1` vs `localhost`, а **новый пустой инстанс**. Восстановление возможно только из **бэкапа**, старого **volume** (`docker volume ls`) или если те же ключи ещё есть в другом Redis (например, не в Docker).

### Переменные окружения

```bash
cp env.docker.example .env
```

- **`KEYDB_DS_URL`** — основной Redis с фичами.
- **`KEYDB_DS_SECOND_URL`** — для `pers_offl`; локально совпадает с основным URL.
- **`FEATURE_RESPONSE_CACHE_URL`** — второй Redis (кэш-aside ответа); пусто = без кэша ответа.

### Запуск API

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Проверочный запрос

```bash
curl -s -X POST http://127.0.0.1:8000/api/v1/features \
  -H "Content-Type: application/json" \
  -d '{
    "brand": "lo",
    "items": [100001, 100002],
    "entries": [{"user_id": 98117045, "store_id": 10}],
    "requested_features": {
      "pers_item": ["ord_60", "price"],
      "pers_user_item": ["pers_pei"],
      "pers_offl": ["offl_ord"]
    }
  }'
```

### Тесты (опционально)

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```
