# `app/services` — кто за что отвечает

## Структура папки

- `registry/` — реестр групп и whitelist признаков (`FeatureRegistry`).
- `pipeline/` — этапы загрузки и сборки ответа (`context`, `city resolution`, `fetch plan`, `loaders`, `assemblers`, `orchestration`).
- `validation/` — правила и валидация входного запроса.

## Pipeline признаков (основной путь `POST /features`)

| Класс / модуль | Назначение |
|----------------|------------|
| **`FeatureRegistry`** | Каталог групп: whitelist имён колонок, `resolve_columns` (пересечение `requested` с `pers_cols` и порядок), внутренние группы (`internal_groups` / `internal_allowed_names`). |
| **`FeatureAccessContext`** + **`build_feature_access_context`** | Неизменяемый снимок запроса: какие группы запрошены, `items`, `brand`, `user_id` / `store_id`, `RequestedFeatures`. |
| **`CityResolutionService`** | Определение города: `store_id` → `get_store_city`, иначе `get_user_cities` → `(city_id, user_cities)`. |
| **`FeatureFetchPlan`** + **`build_feature_fetch_plan`** | План чтения из KeyDB: порядок `city_id` для `pers_user_item`, флаги загрузки `pers_item` / `pers_offl` (без I/O). |
| **`PersUserItemLoader`**, **`PersItemLoader`**, **`PersOfflLoader`** (+ протоколы в `feature_loaders.py`) | Асинхронные обёртки над `FeatureRepository`: последовательные `get_pers_user_item` по городам, `get_pers_item_by_items`, `get_pers_offl`. |
| **`PersItemAssembler`**, **`PersUserItemAssembler`**, **`PersOfflAssembler`** (+ протоколы в `feature_assemblers.py`) | Синхронная сборка JSON-ответа из сырых строк (фильтрация по `items`, суммирование PUI по городам, проекция колонок). |
| **`feature_row_utils`** (`extract_feature_values`, `normalize_feature_row`, …) | Общая работа с векторами признаков (dict vs list, дополнение/обрезка длины). |
| **`FeatureOrchestrationService`** | Склейка: контекст → город → план → кэш `pers_cols` → лоадеры → ассемблеры → `FeatureResponse`. |

## Валидация и правила

| Класс / модуль | Назначение |
|----------------|------------|
| **`FeatureRequestValidator`** | Бизнес-валидация тела после Pydantic: группы, `brand`, `entries`, `items`, имена фич (через `FeatureRegistry`). |
| **`FeatureGroupRule`**, **`FEATURE_GROUP_RULES`** (`validation/group_rules.py`) | Требования к запросу по группе: нужен ли `brand`, непустые `entries`, обязательные поля в `entry`. |
| **`FEATURE_WHITELIST_BY_GROUP`**, **`INTERNAL_FEATURE_WHITELIST`** (`validation/feature_rules.py`) | Исходные списки допустимых имён; публичный whitelist читает **`FeatureRegistry`**, внутренний — отдельно. |

## Экспорт

| Файл | Содержимое |
|------|------------|
| **`__init__.py`** | Публичные точки: `FeatureOrchestrationService`, `FeatureRequestValidator` и протокол валидатора. |
