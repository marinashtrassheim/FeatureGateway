import json
import os
from typing import Any, Dict, List, Set, Tuple, Union

from keydb import KeyDB
from loguru import logger
import mlflow
from mlflow import MlflowException
import numpy as np
import requests


class CommonRankAPIWrapper(mlflow.pyfunc.PythonModel):
    def __init__(self, lo_features_config: dict, lo_model_config: dict, api_address: str):
        """
        :param lo_features_config: параметры признаков модели для ЛО - доставки.
        :param lo_model_config: параметры модели для ЛО - доставки.
        """
        # параметры признаков
        self.feature_parameters = {
            "lo": self._prepare_features_meta(lo_features_config)
        }
        # параметры моделей
        self.model_parameters = {
            "lo": lo_model_config
        }

        # подключения, которые создаются в load_context
        self.db_conn_1 = None
        self.db_conn_2 = None
        # модель TIFUKNN, которая создается в set_models
        self.tifuknn_model = None
        self.tifuknn_users = {}
        self.tifuknn_search_k = {}
        # featurestore api
        self.api_address = api_address
        self.api_session = None

    def load_context(self, context):
        """
        Стандартный метод модели load_context. Загрузка контекста модели.
        :param context: контекст модели.
        """
        # подключение к KeyDB
        try:
            db_host_1 = os.getenv("REDIS_HOST")
            db_port_1 = os.getenv("REDIS_PORT")
            self.db_conn_1 = KeyDB(host=db_host_1,
                                   port=db_port_1,
                                   db=os.getenv("REDIS_DB") or 0,
                                   username=os.getenv("REDIS_USER"),
                                   password=os.getenv("REDIS_PASSWORD"),
                                   charset="utf-8",
                                   decode_responses=True,
                                   socket_timeout=1,
                                   socket_connect_timeout=1)
            logger.debug(f"KeyDB-1 connection is created. Host: {db_host_1}:{db_port_1}")
        except Exception as e:
            raise MlflowException(f"Error in KeyDB-1 connection: {repr(e)}")

        try:
            db_host_2 = os.getenv("REDIS_HOST_2")
            db_port_2 = os.getenv("REDIS_PORT_2")
            self.db_conn_2 = KeyDB(host=db_host_2,
                                   port=db_port_2,
                                   db=os.getenv("REDIS_DB") or 0,
                                   username=os.getenv("REDIS_USER"),
                                   password=os.getenv("REDIS_PASSWORD_2"),
                                   charset="utf-8",
                                   decode_responses=True,
                                   socket_timeout=1,
                                   socket_connect_timeout=1)
            logger.debug(f"KeyDB-2 connection is created. Host: {db_host_1}:{db_port_1}")
        except Exception as e:
            raise MlflowException(f"Error in KeyDB-2 connection: {repr(e)}")

        # featurestore api
        self.api_session = requests.session()

    def set_models(self, tifuknn_model):
        """
        Передаем модель tifuknn_model в класс модели извне.
        """
        self.tifuknn_model = tifuknn_model
        self.tifuknn_users = self.tifuknn_model._model_impl.python_model.model_users

        tifuknn_inner_model = tifuknn_model.unwrap_python_model()
        for retail_brand, model in tifuknn_inner_model.models.items():
            if retail_brand in self.model_parameters:
                self.tifuknn_search_k[retail_brand] = \
                    int(round(model.users_with_vec_count / self.model_parameters[retail_brand]["tifuknn_search_k_div"]))
        logger.debug("Models are loaded")

    def _prepare_features_meta(self, config: dict) -> dict:
        """
        Преобразование параметров признаков в удобный формат.
        :param config: параметры признаков модели.
        :return: параметры из config и новые параметры:
            features_list: List[str] - список всех признаков модели;
            features_weights: np.array - список весов признаков модели в порядке features_list.
        """
        item_features_dict = config.get("item_features_dict", {})
        item_user_features_dict = config.get("item_user_features_dict", {})
        item_user_offline_dict = config.get("item_user_offline_dict", {})
        other_features_dict = config.get("other_features_dict", {})
        features_list = list(item_features_dict.keys()) + \
                        list(item_user_features_dict.keys()) + \
                        list(item_user_offline_dict.keys()) + \
                        list(other_features_dict.keys())
        features_weights = np.array(list(item_features_dict.values()) +
                                    list(item_user_features_dict.values()) +
                                    list(item_user_offline_dict.values()) +
                                    list(other_features_dict.values()))
        return {"features_list": features_list, "features_weights": features_weights, **config}

    def _parse_input(self, model_input) -> Tuple[str, int, int, List[int]]:
        """
        Обработка входящих параметров.
        :param model_input: входящие параметры модели.
        :return:
            retail_brand: бренд,
            user_id: ид. клиента,
            store_id: ид. ТК,
            items: список ид. товаров.
        """
        # входные данные
        retail_brand = model_input["retail_brand"]
        user_id = model_input["user_id"]
        store_id = model_input.get("store_id", -1)
        items = model_input["items"]

        return retail_brand, user_id, store_id, items

    def predict(self, context, model_input) -> List[Dict[str, Any]]:
        """
        Стандартный метод модели predict. Прогноз модели.
        :param context: контекст модели.
        :param model_input: входные параметры модели.
        """
        retail_brand, user_id, store_id, items = self._parse_input(model_input)

        # есть только модель для ЛО
        if retail_brand not in ["lo"]:
            return []

        # если нет товаров, то возвращаем пустой ответ
        if not items:
            return []

        # не делаем никаких расчетов, если 1 товар
        if len(items) == 1:
            return [{"item_id": items[0], "score": 1}]

        # сбор всех признаков
        features = self.get_features(retail_brand, user_id, store_id, items)
        # нормализация признаков, где это необходимо
        # (кроме other features, которые идут в конце и уже как надо обработаны)
        other_features_num = len(self.feature_parameters[retail_brand]["other_features_dict"])
        if other_features_num:
            features_norm = np.concatenate([self.features_normalization(features[:, :-other_features_num]),
                                            features[:, -other_features_num:]], axis=1)
        else:
            features_norm = self.features_normalization(features)
        # получение скоров для ранжирования
        score = self.linear_predict(retail_brand, features_norm)

        result = [{"item_id": item, "score": score}
                  for item, score in zip(items, score)]
        result.sort(key=lambda x: x["score"], reverse=True)
        return result

    def get_features(self, retail_brand: str, user_id: int, store_id: int, items: List[int]) -> np.array:
        """
        Сбор признаков для модели.
        :param retail_brand: бренд.
        :param user_id: ид. клиента.
        :param store_id: ид. ТК.
        :param items: список ид. товаров.
        :return:
            features: np.array - признаки товаров (в порядке items).
        """
        n_items = len(items)

        params = self.feature_parameters[retail_brand]
        item_feature_names = list(params["item_features_dict"].keys())
        item_user_feature_names = list(params["item_user_features_dict"].keys())
        item_user_offline_feature_names = list(params["item_user_offline_dict"].keys())
        other_feature_names = list(params["other_features_dict"].keys())

        api_request = {
            "brand": retail_brand,
            "items": items,
            "entries": [{
                "user_id": user_id,
                "store_id": store_id,
            }],
            "requested_features": {
                "pers_item": item_feature_names,
                "pers_user_item": item_user_feature_names,
                "pers_offl": item_user_offline_feature_names,
            }
        }
        try:
            api_response = self.api_session.post(self.api_address, json=api_request)
            if api_response.status_code == 200:
                api_response = json.loads(api_response.text)
            else:
                api_response = {}
        except Exception as e:  # TODO
            logger.error(e)
            api_response = {}

        if not api_response:
            raise MlflowException("Can't get features from API")  # TODO

        feature_groups = [
            ("pers_item", item_feature_names),
            ("pers_user_item", item_user_feature_names),
            ("pers_offl", item_user_offline_feature_names),
        ]
        expected_feature_names = [
            feature_name
            for _, feature_names in feature_groups
            for feature_name in feature_names
        ] + other_feature_names
        if expected_feature_names != params["features_list"]:
            raise MlflowException("Feature order in get_features doesn't match features_list")

        feature_count = len(expected_feature_names)
        features = np.zeros((n_items, feature_count), dtype=float)

        try:
            api_features = api_response["features"]
        except KeyError as e:
            raise MlflowException(f"Can't find features in API response: {str(e)}")

        item_keys = [str(item) for item in items]
        col_start = 0
        for group_name, feature_names in feature_groups:
            group_features = api_features.get(group_name, {})
            for row_idx, item_key in enumerate(item_keys):
                item_values = group_features.get(item_key, {})
                for col_offset, feature_name in enumerate(feature_names):
                    features[row_idx, col_start + col_offset] = item_values.get(feature_name) or 0.0
            col_start += len(feature_names)

        tifuknn_score = self._get_tifuknn_score(retail_brand, user_id, set(items))
        tifuknn_power_div = self.model_parameters[retail_brand]["tifuknn_power_div"]
        other_feature_values = {
            "tifuknn_score": np.array([
                tifuknn_score.get(item, 0.0) ** (1 / tifuknn_power_div)
                for item in items], dtype=float),
            "position": np.arange(n_items, 0, -1, dtype=float) / n_items,
        }
        for feature_name in other_feature_names:
            if feature_name not in other_feature_values:
                raise MlflowException(f"Unsupported other feature: {feature_name}")
            features[:, col_start] = other_feature_values[feature_name]
            col_start += 1
        return features

    def features_normalization(self, features: np.array,
                               global_min: float = 0.0, global_max: float = 1.0) -> np.array:
        """
        Приведение признаков в диапазон [global_min, global_max].
        :param features: признаки товаров в нужном порядке.
        :param global_min: минимальная граница диапазона.
        :param global_max: максимальная граница диапазона.
        :return: нормализованные признаки товаров.
        """
        features_min = features.min(axis=0)
        features_max = features.max(axis=0)
        divisor = (features_max - features_min)

        features_norm = np.divide(features - features_min, divisor,
                                  out=np.zeros_like(features), where=divisor != 0) \
                        * (global_max - global_min) + global_min
        return features_norm

    def linear_predict(self, retail_brand: str, features_norm: np.array) -> np.array:
        """
        Прогноз линейной модели.
        :param retail_brand: бренд.
        :param features: признаки товаров в нужном порядке, уже нормализованные при необходимости.
        :return: скоры модели.
        """
        score = np.dot(features_norm, self.feature_parameters[retail_brand]["features_weights"])
        return score

    def _filter_features(self, feature_names: List[str], features_values: Dict[str, str],
                         features_dict: Dict[str, float], items_iterable: Union[List, Set]) \
            -> Dict[int, List[Union[int, float]]]:
        """
        Обработка полученных признаков: фильтрация по товарам и полям, приведение типов.
        :param feature_names: название колонок в redis.
        :param features_values: полный набор признаков по товарам из Redis, тип string.
        :param features_dict: набор необходимых признаков и их весов для модели.
        :param items_iterable: список ид. товаров.
        """
        result = {int(item): json.loads(feats)
                  for item, feats in features_values.items()
                  if int(item) in items_iterable}
        try:
            feature_idx = [feature_names.index(col) for col in features_dict.keys()]
        except ValueError as e:
            raise MlflowException(f"Can't find feature name in columns. {str(e)}")

        result = {item: [feats[i] for i in feature_idx]
                  for item, feats in result.items()}
        return result

    def _get_tifuknn_score(self, retail_brand: str, user_id: int, items_set: Set[int]) -> Dict[int, float]:
        """
        Получение скоров модели рекомендации TIFUKNN.
        :param retail_brand: бренд.
        :param user_id: ид. клиента.
        :param items_set: список ид. товаров - set.
        :return: dict:
            key: int - ид. товара,
            value: float - скор модели.
        """
        model_input = {
            "retail_brand": retail_brand,
            "user_id": user_id,
            "item_ids": items_set,
            "search_k": self.tifuknn_search_k[retail_brand],
            "raw_result": True,
        }
        recom = {}
        try:
            recom = self.tifuknn_model.predict(model_input)
        except Exception as e:
            logger.warning(f"Error getting Common recommendation for user '{user_id}'. {str(e)}")
        return recom
