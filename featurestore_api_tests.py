#!/usr/bin/env python
# coding: utf-8

# In[1]:


# keydb==0.0.1
# redis==4.5.4
# pandas==2.0.2
# numpy==1.24.2
# mlflow==2.14.1
# loguru==0.7.3
# annoy==1.17.3
# joblib==1.4.2
# scikit-learn==1.2.2
# scipy==1.10.1
# deepdiff==8.6.2
# requests==2.28.2


# In[1]:


import json
import os
import time
from typing import Dict, List, Tuple

from deepdiff import DeepDiff
from keydb import KeyDB
from loguru import logger
import mlflow
import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

os.environ["MLFLOW_TRACKING_URI"] = "https://mlflow.ds.lenta.tech/"
os.environ["MLFLOW_TRACKING_USERNAME"] = "admin"
os.environ["MLFLOW_TRACKING_PASSWORD"] = "Yooro8quohbi"


# In[2]:


api_address = "http://srv-jupyterhub-ds-personalization.lenta.tech:8000/api/v1/features"


# # Подключение к БД

# In[3]:


keydb_params = {
    "host": "srv-jupyterhub-ds-personalization.lenta.tech", "port": "6377",
    "db": 0, "password": "password2", 
    "charset": "utf-8", "decode_responses": True
}
keydb_conn = KeyDB(**keydb_params)


# Проверка параметров подключения

# In[4]:


keydb_conn.config_get("hash-max-ziplist-entries"), keydb_conn.config_get("hash-max-ziplist-value")


# In[5]:


keydb_conn.info("memory")
# при hash-max-ziplist-value=64 used_memory_human=28.26G (но возможно был оверхед после записи, надо перепроверить)


# # Подготовка моделей

# In[6]:


from models.common_rank_wrapper import CommonRankWrapper


# In[29]:


get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')
from models.common_rank_wrapper_api import CommonRankAPIWrapper


# In[8]:


# модель-зависимость
tifuknn_model = mlflow.pyfunc.load_model("runs:/bd29d83f4c0746d289e8b6381c277655/common_recommender")


# In[9]:


# параметры работы модели
MODEL_PARAMS = {
    "lo_features_config": {
        "item_features_dict": {"ord_14": 0.08},
        "item_user_features_dict": {"wgh_pers_ord": 0.6},
        "item_user_offline_dict": {"offl_ord": 0.2},
        "other_features_dict": {"tifuknn_score": 0.3, "position": 0.01}
    },
    "lo_model_config": {
        "tifuknn_power_div": 3,
        "tifuknn_search_k_div": 100,
    }
}


# In[10]:


# параметры подключений для модели передаются через переменные окружения
os.environ["REDIS_HOST"] = keydb_params["host"]
os.environ["REDIS_PORT"] = keydb_params["port"]
os.environ["REDIS_PASSWORD"] = keydb_params["password"]
os.environ["REDIS_DB"] = str(keydb_params["db"])

os.environ["REDIS_HOST_2"] = keydb_params["host"]
os.environ["REDIS_PORT_2"] = keydb_params["port"]
os.environ["REDIS_PASSWORD_2"] = keydb_params["password"]


# In[11]:


# текущая версия модели с подключением напрямую в KeyDB
current_model = CommonRankWrapper(MODEL_PARAMS["lo_features_config"], MODEL_PARAMS["lo_model_config"])
current_model.load_context(None)
current_model.set_models(tifuknn_model)


# In[30]:


# версия с подключенным API
api_model = CommonRankAPIWrapper(MODEL_PARAMS["lo_features_config"], MODEL_PARAMS["lo_model_config"],
                                 api_address)
api_model.load_context(None)
api_model.set_models(tifuknn_model)


# # Подготовка тестовых данных
# Данные o действиях клиентов в каталоге, к которым также можно применить модель общего ранжирования (common rank)

# In[13]:


test_set_raw = pd.read_parquet("data/test_set_20260505.parquet")


# In[14]:


test_set_raw.head()


# In[15]:


test_set_raw.shape, test_set_raw.user_id.nunique(), test_set_raw.item_id.nunique()


# In[16]:


# агрегируем тестовые данные: одна строка = один запрос к модели
key_cols = ["event_date", "hub_id", "user_id", "user_pseudo_id", "category_id"]
test_set = test_set_raw.groupby(key_cols, as_index=False, dropna=False).agg(
    items=("item_id", lambda x: list(x))
).merge(
    test_set_raw[test_set_raw["target"] == 1.0] \
        .groupby(key_cols, as_index=False, dropna=False).agg(y_true=("item_id", lambda x: set(x))
    )
)
test_set.shape


# In[17]:


test_set.head()


# In[18]:


# распределение количества ранжируемых товаров в запросах
test_set["items"].apply(len).describe(percentiles=[0.25,0.5,0.75,0.9,0.95])


# # Тесты

# ## Оценка времени ответа и различий в ответах

# In[23]:


def get_predictions(model, test_set: pd.DataFrame) -> Tuple[Dict[tuple, dict], List[float]]:
    """
    Получение ответов модели (отранжированных списков товаров) по данным test_set и замер скорости ответа в мс.
    """
    prediction_results = {}
    prediction_latencies = []
    for _, row in tqdm(test_set.iterrows(), total=test_set.shape[0]):
        model_input = {
            "retail_brand": "lo",
            "user_id": int(row["user_id"]) if not pd.isna(row["user_id"]) else 0,
            "store_id": int(row["hub_id"]),
            "items": row["items"]
        }
        start = time.perf_counter()
        try:
            pred = model.predict(None, model_input)
        except Exception as exc:
            logger.warning(f"Prediction failed for row={row}: {exc}")
            pred = {}
        latency_ms = (time.perf_counter() - start) * 1000 
        
        prediction_key = tuple(row[col] for col in key_cols)
        prediction_results[prediction_key] = pred
        prediction_latencies.append(latency_ms)
    return prediction_results, prediction_latencies


def latency_report(prediction_latencies, percentiles=[0.25,0.5,0.75,0.9,0.95,0.99]):
    """
    Статистика по времени ответа в мс.
    """
    print(pd.DataFrame(prediction_latencies).describe(percentiles).round(3))
    
    
def compare_predictions(prediction_1: Dict[tuple, dict], prediction_2: Dict[tuple, dict]):
    """
    Сравнение двух ответов (на вход передается ответ от get_predictions - prediction_results)
    """
    diff = DeepDiff(prediction_1, prediction_2)
    if diff:
        print("⚠️ Есть различия!")
    else:
        print("✅ Структуры идентичны")
    return diff


# In[24]:


prediction_current_results, prediction_current_latencies = get_predictions(current_model, test_set.head(1000))


# In[31]:


prediction_api_results, prediction_api_latencies = get_predictions(api_model, test_set.head(1000))


# In[32]:


latency_report(prediction_current_latencies)


# In[33]:


latency_report(prediction_api_latencies)


# In[34]:


predicition_diff = compare_predictions(prediction_current_results, prediction_api_results)


# ## Оценка размера ключей в базе

# In[42]:


def show_prefix_info(keys, sort_by_value=False) -> Dict[str, int]:
    """
    Группировка ключей по префиксам.
    """
    info = {str(k): int(v) for k, v in zip(*np.unique([x.split(':')[0] for x in keys], return_counts=True))}
    if sort_by_value:
        info = dict(sorted(info.items(), key=lambda x: x[1], reverse=True))
    return info

def prefix_keys(keys, prefix) -> List[str]:
    """
    Фильтрация всех ключей по заданному префиксу.
    """
    return [x for x in keys if x.startswith(f"{prefix}:")]

def get_memory_by_prefix(db_conn, keys, prefix, keys_limit: int = None):
    """
    Получение статистики о размере ключей по заданному префиксу.
    """
    pr_keys = prefix_keys(keys, prefix)
    if keys_limit:
        pr_keys = pr_keys[:keys_limit]
    n_keys = len(pr_keys)
    print(f"Prefix keys = {n_keys}")
    
    prefix_mem = 0
    for key in tqdm(pr_keys):
        key_mem = db_conn.memory_usage(key)
        prefix_mem += key_mem
        
    print(f"{prefix}")
    print(f"Full memory usage: {prefix_mem/1024/1024:.3f} Mb ({prefix_mem})")
    print(f"Avg memory by key: {prefix_mem/n_keys/1024:.3f} Kb ({n_keys} keys)")


# In[43]:


keys = keydb_conn.keys()


# In[46]:


show_prefix_info(keys)


# In[47]:


get_memory_by_prefix(keydb_conn, keys, "pers_user_item", keys_limit=None)


# In[ ]:


# при hash-max-ziplist-value=64
# pers_user_item
# Full memory usage: 17432.090 Mb (18278871192)
# Avg memory by key: 5.134 Kb (3476605 keys)

