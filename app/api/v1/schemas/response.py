from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class FeaturesBlock(BaseModel):
    """
    Ключи верхнего уровня — item_id как строки (как поля hash в KeyDB / JSON object keys).
    """

    model_config = ConfigDict(extra="forbid")

    pers_item: dict[str, dict[str, Any]] | None = Field(
        default=None,
        description="Признаки товара по городу (или city=-1), ключи: item_id строкой.",
    )
    pers_user_item: dict[str, dict[str, Any]] | None = Field(
        default=None,
        description="Признаки товар+пользователь. При нескольких городах пользователя может быть агрегировано суммой.",
    )
    pers_offl: dict[str, dict[str, Any]] | None = Field(
        default=None,
        description="Оффлайн-признаки товар+пользователь.",
    )


class MetadataResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    aggregated_cities: list[int] = Field(
        default_factory=list,
        description=(
            "Список городов, использованных для контекста пользователя. "
            "Для pers_user_item без store_id обычно содержит города из pers_user_city."
        ),
        examples=[[53, 86]],
    )


class FeatureResponse(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "features": {
                    "pers_item": {
                        "720704": {
                            "ord_365": 35427,
                            "ord_60": 7642,
                            "pei_60": 8522,
                            "ord_14": 6,
                            "ord_365_total": 36713,
                            "ord_60_total": 7988,
                            "pei_60_total": 8932,
                            "ord_14_total": 6,
                            "margin": 0,
                            "m2": 0,
                            "price": 0,
                            "discount_rub": 0,
                            "discount_prt": 0,
                            "m2_prt": 0,
                        }
                    },
                    "pers_user_item": {
                        "720704": {
                            "pers_pei": 2,
                            "pers_ord": 2,
                            "pers_revenue": 359.98,
                            "min_price": 359.98,
                            "diff_coef": 542,
                            "wgh_pers_ord": 0.08,
                            "std_price": 0,
                            "diff_ord": 0,
                            "reg_b": 2,
                            "items_b": 16,
                            "rev_coef": 0.168,
                            "pei_coef": 0.182,
                            "perc_return": 0,
                            "perc_discount": 0,
                            "first_b": 2,
                            "last_b": 2,
                        }
                    },
                    "pers_offl": {
                        "720704": {"offl_ord": 41, "offl_pei": 17}
                    },
                },
                "metadata": {"aggregated_cities": [53, 86]},
            }
        },
    )

    features: FeaturesBlock
    metadata: MetadataResponse = Field(default_factory=MetadataResponse)
