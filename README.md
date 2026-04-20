# Feature Gateway

API-шлюз между ML-модулями и KeyDB/Redis для групп признаков `pers_item`, `pers_user_item`, `pers_offl`.

## Локальный запуск 

Три отдельных Redis в Docker на портах хоста **7379 / 7380 / 7381** (v1 / v2 / v3). Дальше — клонирование, seed и проверочный запрос.

### 1. Зависимости

- Python 3.10+
- Docker 

### 2. Клонирование и виртуальное окружение

```bash
git clone git@github.com:marinashtrassheim/FeatureGateway.git 
cd FeatureGateway
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Поднять три Redis (v1 / v2 / v3)
Почему у нас 3 Redis? Потому что в проекте я тестировала разные варианты хранения данных в онлайн хранилище, затем тестировала выполнение запросов. Файлы выложены в https://drive.google.com/drive/folders/1p7FLw34R7v7sZ9NwRRiZ0EDIZrTXBKGX?usp=sharing

```bash
docker compose up -d
```

На хосте:

| Роль в коде | URL в примере `.env` |
|-------------|----------------------|
| Хранилище v1 | `redis://127.0.0.1:7379/0` |
| Хранилище v2 | `redis://127.0.0.1:7380/0` |
| Хранилище v3 | `redis://127.0.0.1:7381/0` |

### 4. Переменные окружения

```bash
cp env.docker.example .env
```

- **`STORAGE_VERSION`** — `v1`, `v2` или `v3`.
- **`KEYDB_V1_URL` / `KEYDB_V2_URL` / `KEYDB_V3_URL`** — как в таблице выше.
- **`KEYDB_DS_SECOND_URL`** — тот же хост и порт, что и основное хранилище для **выбранной** версии (`pers_offl` в том же Redis; в приложении два клиента к одному инстансу).

Пример для проверки **v1**:

```env
STORAGE_VERSION=v1
KEYDB_V1_URL=redis://127.0.0.1:7379/0
KEYDB_V2_URL=redis://127.0.0.1:7380/0
KEYDB_V3_URL=redis://127.0.0.1:7381/0
KEYDB_DS_URL=redis://127.0.0.1:7379/0
KEYDB_DS_SECOND_URL=redis://127.0.0.1:7379/0
```

Для **v2**: `STORAGE_VERSION=v2` и `KEYDB_DS_SECOND_URL=redis://127.0.0.1:7380/0` (и при необходимости `KEYDB_DS_URL` на тот же URL).

`FEATURE_RESPONSE_CACHE_URL` оставьте пустым, если отдельный Redis под кэш ответа не нужен.

### 5. Загрузить демо-данные (seed)

Один раз — данные под все три формата хранения (отдельная БД на каждый порт):

```bash
python scripts/seed_demo_data.py --all --flush
```

Только одна версия:

```bash
python scripts/seed_demo_data.py --version v1 --flush
```

Порты по умолчанию в скрипте совпадают с `docker-compose.yml` (7379 / 7380 / 7381).

### 6. Запуск API

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 7. Проверочный запрос

Тело запроса: `scripts/demo_request.json`.

```bash
python scripts/smoke_features_request.py
```

или:

```bash
curl -s -X POST http://127.0.0.1:8000/api/v1/features \
  -H "Content-Type: application/json" \
  -d @scripts/demo_request.json
```

Ожидается **200** и непустые блоки в `features`, если `STORAGE_VERSION`, `.env` и засидированный порт согласованы.

### 8. Тесты (опционально)

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```
