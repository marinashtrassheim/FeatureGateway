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


class FeatureStorageError(Exception):
    """Базовая ошибка доступа к хранилищу признаков (KeyDB)."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "FEATURE_STORAGE_ERROR",
        operation: str | None = None,
        key: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.operation = operation
        self.key = key


class FeatureStorageUnavailableError(FeatureStorageError):
    """Хранилище признаков недоступно (timeout/connectivity)."""

    def __init__(self, message: str, *, operation: str | None = None, key: str | None = None) -> None:
        super().__init__(
            message,
            code="FEATURE_STORAGE_UNAVAILABLE",
            operation=operation,
            key=key,
        )


class FeatureStorageDataFormatError(FeatureStorageError):
    """Некорректный формат данных в хранилище признаков."""

    def __init__(self, message: str, *, operation: str | None = None, key: str | None = None) -> None:
        super().__init__(
            message,
            code="FEATURE_STORAGE_DATA_FORMAT_ERROR",
            operation=operation,
            key=key,
        )
