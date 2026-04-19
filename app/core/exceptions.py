from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ValidationErrorItem:
    """Один пункт ошибки валидации (совместимо с полем errors в ответе API)."""

    loc: tuple[str | int, ...]
    msg: str

    def as_dict(self) -> dict[str, Any]:
        return {"loc": list(self.loc), "msg": self.msg}


class FeatureValidationError(Exception):
    """Бизнес-валидация запроса фич (после прохождения Pydantic)."""

    def __init__(
        self,
        message: str,
        errors: list[ValidationErrorItem] | None = None,
        *,
        code: str = "VALIDATION_ERROR",
    ) -> None:
        super().__init__(message)
        self.message = message
        self.errors: list[ValidationErrorItem] = list(errors or [])
        self.code = code

    def errors_as_dicts(self) -> list[dict[str, Any]]:
        return [e.as_dict() for e in self.errors]
