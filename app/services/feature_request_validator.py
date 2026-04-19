from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.api.v1.schemas.request import FeatureRequest
from app.core.exceptions import FeatureValidationError, ValidationErrorItem
from app.services.validation.feature_rules import FEATURE_WHITELIST_BY_GROUP
from app.services.validation.group_rules import FEATURE_GROUP_RULES, FeatureGroupRule


@runtime_checkable
class FeatureRequestValidatorProtocol(Protocol):
    def validate(self, request: FeatureRequest) -> None: ...


class FeatureRequestValidator:
    """
    Бизнес-валидация после Pydantic.

    Правила по группам — в FEATURE_GROUP_RULES (расширение без новых if по именам групп).
    Whitelist имён признаков — в FEATURE_WHITELIST_BY_GROUP.
    """

    def validate(self, request: FeatureRequest) -> None:
        errors: list[ValidationErrorItem] = []
        groups = self._requested_api_groups(request)

        if not groups:
            errors.append(
                ValidationErrorItem(
                    ("body", "requested_features"),
                    "Укажите хотя бы одну группу признаков (pers_user_item, pers_item или pers_offl).",
                )
            )
            self._raise(errors)
            return

        active_rules = self._active_rules(groups)
        self._validate_unknown_groups(groups, errors)
        self._validate_brand(request, active_rules, errors)
        self._validate_feature_names(request, groups, errors)
        self._validate_entries(request, active_rules, errors)
        self._validate_items_required(request, errors)
        self._validate_pers_item_request(request, groups, errors)

        self._raise(errors)

    def _requested_api_groups(self, request: FeatureRequest) -> set[str]:
        rf = request.requested_features
        out: set[str] = set()
        if rf.pers_user_item is not None:
            out.add("pers_user_item")
        if rf.pers_item is not None:
            out.add("pers_item")
        if rf.pers_offl is not None:
            out.add("pers_offl")
        return out

    def _active_rules(self, groups: set[str]) -> list[FeatureGroupRule]:
        return [
            FEATURE_GROUP_RULES[k]
            for k in sorted(groups)
            if k in FEATURE_GROUP_RULES
        ]

    def _validate_unknown_groups(
        self, groups: set[str], errors: list[ValidationErrorItem]
    ) -> None:
        unknown = groups - set(FEATURE_GROUP_RULES.keys())
        for gid in sorted(unknown):
            errors.append(
                ValidationErrorItem(
                    ("body", "requested_features", gid),
                    f'Группа «{gid}» не зарегистрирована в правилах шлюза.',
                )
            )

    def _validate_brand(
        self,
        request: FeatureRequest,
        active_rules: list[FeatureGroupRule],
        errors: list[ValidationErrorItem],
    ) -> None:
        needing = [r.id for r in active_rules if r.requires_brand]
        if needing and request.brand is None:
            errors.append(
                ValidationErrorItem(
                    ("body", "brand"),
                    "Для групп "
                    + ", ".join(sorted(needing))
                    + " необходимо указать brand (lo, mntk или utk).",
                )
            )

    def _validate_feature_names(
        self,
        request: FeatureRequest,
        groups: set[str],
        errors: list[ValidationErrorItem],
    ) -> None:
        rf = request.requested_features
        for group_key in groups:
            allowed = FEATURE_WHITELIST_BY_GROUP.get(group_key)
            if not allowed:
                continue

            names = getattr(rf, group_key, None)
            if names is None or len(names) == 0:
                continue

            for name in names:
                if name not in allowed:
                    errors.append(
                        ValidationErrorItem(
                            ("body", "requested_features", group_key),
                            f'Фича «{name}» не существует в хранилище для группы «{group_key}».',
                        )
                    )

    def _validate_entries(
        self,
        request: FeatureRequest,
        active_rules: list[FeatureGroupRule],
        errors: list[ValidationErrorItem],
    ) -> None:
        needs_rows = any(r.requires_non_empty_entries for r in active_rules)
        if not request.entries:
            if needs_rows:
                needing = sorted(
                    r.id for r in active_rules if r.requires_non_empty_entries
                )
                errors.append(
                    ValidationErrorItem(
                        ("body", "entries"),
                        "Для групп "
                        + ", ".join(needing)
                        + " необходим хотя бы один контекст (entries).",
                    )
                )
            return

        for entry_index, entry in enumerate(request.entries):
            for rule in active_rules:
                for field in rule.required_entry_fields:
                    if getattr(entry, field, None) is None:
                        errors.append(
                            ValidationErrorItem(
                                ("body", "entries", entry_index, field),
                                f"Для группы {rule.id} обязательно укажите {field}.",
                            )
                        )

    def _validate_items_required(
        self,
        request: FeatureRequest,
        errors: list[ValidationErrorItem],
    ) -> None:
        """Как в common_wrank_model: список items обязателен для всех групп фич."""
        if request.items:
            return
        errors.append(
            ValidationErrorItem(
                ("body", "items"),
                "Укажите непустой список items (как во входе common_wrank_model).",
            )
        )

    def _validate_pers_item_request(
        self,
        request: FeatureRequest,
        groups: set[str],
        errors: list[ValidationErrorItem],
    ) -> None:
        """pers_item: только товары из items; город шлюз берёт из hub / pers_user_city — нужен user_id или store_id."""
        if "pers_item" not in groups:
            return
        if not request.entries:
            return
        e0 = request.entries[0]
        has_store = e0.store_id is not None and e0.store_id != -1
        has_user = e0.user_id is not None
        if not has_store and not has_user:
            errors.append(
                ValidationErrorItem(
                    ("body", "entries", 0),
                    "Для pers_item укажите user_id или store_id (для определения города через pers_hub_city / pers_user_city).",
                )
            )

    def _raise(self, errors: list[ValidationErrorItem]) -> None:
        if not errors:
            return
        message = errors[0].msg
        if len(errors) > 1:
            message = f"Обнаружено ошибок: {len(errors)}. Первая: {errors[0].msg}"
        raise FeatureValidationError(message=message, errors=errors)
