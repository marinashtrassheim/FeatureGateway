from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.core.constants import Brand, Channel


class RequestedFeatures(BaseModel):
    """Запрошенные группы признаков. Отсутствие ключа = группа не запрашивается."""

    model_config = ConfigDict(extra="forbid")

    pers_user_item: list[str] | None = Field(
        default=None,
        description=(
            "Признаки группы pers_user_item. [] = вернуть все поля группы. "
            "Примеры: pers_pei, pers_ord, pers_revenue, min_price, diff_coef."
        ),
        examples=[["pers_pei", "pers_ord", "pers_revenue"], []],
    )
    pers_item: list[str] | None = Field(
        default=None,
        description=(
            "Признаки группы pers_item. [] = вернуть все поля группы. "
            "Примеры: ord_60, price, margin, discount_prt."
        ),
        examples=[["ord_60", "price", "margin"], []],
    )
    pers_offl: list[str] | None = Field(
        default=None,
        description=(
            "Признаки группы pers_offl. [] = вернуть все поля группы. "
            "Примеры: offl_ord, offl_pei."
        ),
        examples=[["offl_ord", "offl_pei"], []],
    )

    @model_validator(mode="after")
    def at_least_one_group(self) -> RequestedFeatures:
        if any(
            x is not None
            for x in (self.pers_user_item, self.pers_item, self.pers_offl)
        ):
            return self
        raise ValueError(
            "Укажите хотя бы одну группу в requested_features "
            "(pers_user_item, pers_item или pers_offl)."
        )


class FeatureEntry(BaseModel):
    """
    Контекст пользователя (user_id, опционально ТЦ).

    Город в KeyDB не передаём: шлюз сам берёт его из pers_hub_city при наличии store_id,
    иначе из pers_user_city по user_id (как в common_rank_wrapper).
    При переданном store_id клиентский city не используется (в API поля city нет).
    """

    model_config = ConfigDict(extra="forbid")

    user_id: int | None = Field(
        default=None,
        description=(
            "ID пользователя. Для pers_user_item и pers_offl обязателен. "
            "Для pers_item используется для получения городов пользователя, "
            "если не передан store_id."
        ),
        examples=[98339593],
    )
    store_id: int | None = Field(
        default=None,
        description=(
            "ID торгового центра. Опционален. Если передан, город берется через "
            "pers_hub_city:{store_id} и имеет приоритет."
        ),
        examples=[10],
    )
    search_query_norm: str | None = Field(
        default=None,
        description="Нормализованный поисковый запрос (опционально, для будущих сценариев).",
        examples=["молоко"],
    )
    channel: Channel | None = Field(
        default=None,
        description="Канал заказа (опционально, для будущих сценариев).",
        examples=["delivery"],
    )


class FeatureRequest(BaseModel):
    """Тело POST /api/v1/features."""

    brand: Brand | None = Field(
        default=None,
        description="Бренд (обязателен для pers_item и pers_user_item). Допустимые значения: lo, mntk, utk.",
        examples=["lo"],
    )
    items: list[int] | None = Field(
        default=None,
        description="Список item_id. Обязателен для текущего контракта. Фильтрация ответа выполняется строго по items.",
        examples=[[720704, 725007]],
    )
    entries: list[FeatureEntry] = Field(
        default_factory=list,
        description=(
            "Контексты запроса. Для текущего контракта поддерживается ровно один "
            "контекст: len(entries) == 1."
        ),
    )
    requested_features: RequestedFeatures

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "brand": "lo",
                "items": [720704, 725007],
                "entries": [
                    {
                        "user_id": 98339593,
                        "store_id": 10,
                        "search_query_norm": "молоко",
                        "channel": "delivery",
                    }
                ],
                "requested_features": {
                    "pers_user_item": ["pers_pei", "pers_ord", "pers_revenue"],
                    "pers_item": ["ord_60", "price", "margin"],
                    "pers_offl": ["offl_ord", "offl_pei"],
                },
            }
        },
    )

    @field_validator("items", mode="before")
    @classmethod
    def normalize_items(cls, v: list[int] | None) -> list[int] | None:
        if v is not None and len(v) == 0:
            return None
        return v
