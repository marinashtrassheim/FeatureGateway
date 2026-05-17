import json
from operator import add
import os
from typing import Any, Dict, List, Set, Tuple, Union

from keydb import KeyDB
from loguru import logger
import mlflow
from mlflow import MlflowException
import numpy as np


class CommonRankWrapper(mlflow.pyfunc.PythonModel):
    def __init__(self, lo_features_config: dict, lo_model_config: dict):
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
        # (кроме tifuknn и позиции, которые идут в конце и уже как надо обработаны)
        features_norm = np.concatenate([self.features_normalization(features[:, :-2]),
                                        features[:, -2:]], axis=1)
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
        items_set = set(items)

        # основные признаки по всем группам из KeyDB
        city_id, all_feature_names, item_user_offline_features, item_user_city_features, item_features = \
            self._get_keydb_features(retail_brand, user_id, store_id, items)

        # название признаков по всем группам
        all_feature_names = {k: json.loads(v) for k, v in all_feature_names.items()}
        try:
            item_cols = all_feature_names["pers_item"]
            item_user_cols = all_feature_names["pers_user_item"]
            item_user_offline_cols = all_feature_names["pers_offl"]
        except KeyError as e:
            raise MlflowException(f"Can't find columns for features in KeyDB: {str(e)}")

        # сбор признаков по группам
        item_features = self._get_item_features(retail_brand, item_features, items, item_cols)
        item_user_features = self._get_user_item_features(retail_brand, item_user_city_features,
                                                          items_set, item_user_cols)
        item_user_offline_features = self._get_user_offline_features(item_user_offline_features,
                                                                     items_set, item_user_offline_cols)
        tifuknn_score = self._get_tifuknn_score(retail_brand, user_id, items_set)

        item_features_num = len(self.feature_parameters[retail_brand]["item_features_dict"])
        item_user_features_num = len(self.feature_parameters[retail_brand]["item_user_features_dict"])
        item_user_offline_features_num = len(self.feature_parameters[retail_brand]["item_user_offline_dict"])
        n_items = len(items)
        features = np.array(
            [item_features.get(item, [0] * item_features_num) +
             item_user_features.get(item, [0] * item_user_features_num) +
             item_user_offline_features.get(item, [0] * item_user_offline_features_num) +
             # other features, be careful with their order!
             [tifuknn_score.get(item, 0) ** (1/self.model_parameters[retail_brand]["tifuknn_power_div"])] +
             [(n_items - i) / n_items]  # = normalized reversed position
             for i, item in enumerate(items)], dtype=float)
        return features

    def _get_keydb_features(self, retail_brand: str, user_id: int, store_id: int, items: List[int]) \
            -> Tuple[int, Dict[str, str], Dict[str, str], List[Dict[str, str]], List[str]]:
        """
        Получение признаков из KeyDB с использованием пайплайнов, где это возможно.
        :param retail_brand: бренд.
        :param user_id: ид. клиента.
        :param store_id: ид. ТК.
        :param items: список ид. товаров.
        :return:
            city_id: int - ид. города,
            all_feature_names: Dict[str, str] - список признаков по группам,
            item_user_offline_features: Dict[str, str] - признаки товаров в офлайне по клиентам,
            item_user_city_features: List[Dict[str, str]] - признаки товаров в онлайне по клиентам,
            item_features: List[str] - признаки товаров по городам.
        """
        # определение города по ТК
        try:
            city_id = self.db_conn_1.get(f"pers_hub_city:{store_id}")
        except Exception as e:
            city_id = -1
            logger.warning(f"Can't get city_id from KeyDB: {str(e)}")

        user_cities = []
        if city_id:
            city_id = int(city_id)
        else:
            city_id = -1
            logger.debug(f"Can't find city for store_id={store_id}. Use default.")

            # если город не найден, то берем признаки клиента из всех его городов
            try:
                user_cities = self.db_conn_1.hget("pers_user_city", str(user_id))
            except Exception as e:
                user_cities = ''
                logger.warning(f"Can't get user cities from KeyDB: {str(e)}")

            if user_cities:
                user_cities = json.loads(user_cities)
            else:
                user_cities = []

        # независимые друг от друга признаки запросим в одном пайплайне
        # выгрузка из KeyDB-1
        with self.db_conn_1.pipeline() as pipe:
            pipe.hgetall(f"pers_cols")

            if city_id == -1 and user_cities:
                user_feat_cn = len(user_cities)
                for city in user_cities:
                    pipe.hgetall(f"pers_user_item:{retail_brand}:{user_id}:{city}")
            else:
                user_feat_cn = 1
                pipe.hgetall(f"pers_user_item:{retail_brand}:{user_id}:{city_id}")

            # для city_id = -1 данные записаны в базу, дополнительной обработки не требуется
            for item in items:
                pipe.hget(f"pers_item:{retail_brand}:{city_id}", str(item))

            try:
                everything = pipe.execute()
            except Exception as e:
                raise MlflowException(f"Can't execute pipeline in first KeyDB: {str(e)}")

        all_feature_names = everything[0]
        item_user_city_features = everything[1:1+user_feat_cn]
        item_features = everything[1+user_feat_cn:]

        # выгрузка из KeyDB-2
        try:
            item_user_offline_features = self.db_conn_2.hgetall(f"pers_offl:{user_id}")
        except Exception as e:
            item_user_offline_features = {}
            logger.warning(f"Can't execute pipeline in second KeyDB: {str(e)}")

        return city_id, all_feature_names, item_user_offline_features, item_user_city_features, item_features

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

    def _get_item_features(self, retail_brand: str, items_features: List[str], items: List[int],
                           feature_names: List[str]) -> Dict[int, List[Union[int, float]]]:
        """
        Обработка признаков товаров по городам.
        :param retail_brand: бренд.
        :param items_features: список признаков товаров из Redis.
        :param items: список ид. товаров.
        :param feature_names: название колонок в redis.
        :return: dict:
            key: int - ид. товара,
            value: List[Union[int, float]] - список значений признаков.
        """
        item_features_dict = self.feature_parameters[retail_brand]["item_features_dict"]
        items_features = {item: json.loads(feats) for item, feats in zip(items, items_features) if feats}

        try:
            feature_idx = [feature_names.index(col) for col in item_features_dict.keys()]
        except ValueError as e:
            raise MlflowException(f"Can't find feature name columns. {str(e)}")

        items_features = {item: [feats[i] for i in feature_idx]
                          for item, feats in items_features.items()}
        return items_features

    def _get_user_item_features(self, retail_brand: str, item_user_city_features: List[Dict[str, str]],
                                items_set: Set[int], feature_names: List[str]) -> Dict[int, List[Union[int, float]]]:
        """
        Обработка признаков товаров по клиентам по городам в ЛО.
        :param retail_brand: бренд.
        :param item_user_city_features: список признаков товаров по клиенту из Redis.
        :param items_set: список ид. товаров - set.
        :param feature_names: название колонок в KeyDB.
        :return: dict:
            key: int - ид. товара,
            value: List[Union[int, float]] - список значений признаков.
        """
        item_user_features_list = [feats for feats in item_user_city_features if feats]
        if not item_user_features_list:
            return {}

        item_user_features_dict = self.feature_parameters[retail_brand]["item_user_features_dict"]
        item_user_features = self._filter_features(feature_names, item_user_features_list[0],
                                                   item_user_features_dict, items_set)
        if len(item_user_features_list) > 1:
            feat_num = len(item_user_features_dict)
            for i in range(1, len(item_user_features_list)):
                features = self._filter_features(feature_names, item_user_features_list[i],
                                                 item_user_features_dict, items_set)
                item_user_features = {item: list(map(add, item_user_features.get(item, [0] * feat_num),
                                                     features.get(item, [0] * feat_num)))
                                      for item in item_user_features.keys() | features.keys()}
        return item_user_features

    def _get_user_offline_features(self, item_user_offline_features: Dict[str, str],
                                   items_set: Set[int], feature_names: List[str]) \
            -> Dict[int, List[Union[int, float]]]:
        """
        Обработка признаков товаров по клиентам в офлайне.
        :param item_user_offline_features: список признаков товаров по клиенту из Redis.
        :param items_set: список ид. товаров - set.
        :param feature_names: название колонок в redis.
        :return: dict:
            key: int - ид. товара,
            value: List[Union[int, float]] - список значений признаков.
        """
        if item_user_offline_features:
            item_user_offline_dict = self.feature_parameters["lo"]["item_user_offline_dict"]
            item_user_offline_features = self._filter_features(feature_names, item_user_offline_features,
                                                               item_user_offline_dict, items_set)
        return item_user_offline_features

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
