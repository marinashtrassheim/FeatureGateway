#!/usr/bin/env python3
"""Seed демо-данных для Feature Gateway (v1 / v2 / v3).

В каждый инстанс пишет (включая pers_offl в тот же Redis, без второго KeyDB на версию):
  • hash pers_cols
  • hash pers_user_city
  • hash pers_user_item:{brand}:{user_id}:{city_id}
  • hash pers_item:{brand}:-1
  • hash pers_offl:{user_id}

В .env: KEYDB_DS_SECOND_URL = тот же хост:порт, что и основной KeyDB для текущей STORAGE_VERSION.

docker-compose.yml (порты 7379/7380/7381):
  python scripts/seed_demo_data.py --all --flush

Одна версия:
  python scripts/seed_demo_data.py --version v2 --flush
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import msgpack
import redis

# пакет app при запуске как scripts/seed_demo_data.py
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from app.core.feature_config import (  # noqa: E402
    PERS_ITEM_MULTIPLIERS,
    PERS_USER_ITEM_MULTIPLIERS,
)

# Совпадает с docker-compose.yml (хост 7379 / 7380 / 7381).
DEFAULT_PORTS = {"v1": 7379, "v2": 7380, "v3": 7381}

BRAND = "lo"

SCENARIOS: list[tuple[int, int, list[int]]] = [
    (98117045, 23, [100001, 100002]),
    (100321838, 70, [200001, 200002]),
]

# Порядок полей = порядок вектора в KeyDB (как в типичном pers_cols продакшена / OpenAPI)
PI_COLS: list[str] = [
    "ord_365",
    "ord_60",
    "pei_60",
    "ord_14",
    "ord_365_total",
    "ord_60_total",
    "pei_60_total",
    "ord_14_total",
    "margin",
    "m2",
    "price",
    "discount_rub",
    "discount_prt",
    "m2_prt",
]

# Целевые значения в ответе API (после декодирования), как в живом примере
PI_DISPLAY: dict[str, float] = {
    "ord_365": 95602,
    "ord_60": 19311,
    "pei_60": 22947,
    "ord_14": 3568,
    "ord_365_total": 100622,
    "ord_60_total": 20395,
    "pei_60_total": 24258,
    "ord_14_total": 3790,
    "margin": 0,
    "m2": 0,
    "price": 0,
    "discount_rub": 0,
    "discount_prt": 0,
    "m2_prt": 0,
}

PUI_COLS: list[str] = [
    "pers_pei",
    "pers_ord",
    "pers_revenue",
    "min_price",
    "diff_coef",
    "wgh_pers_ord",
    "std_price",
    "diff_ord",
    "reg_b",
    "items_b",
    "rev_coef",
    "pei_coef",
    "perc_return",
    "perc_discount",
    "first_b",
    "last_b",
]

# Человекочитаемые значения (как в ответе API после decode)
PUI_DISPLAY: dict[str, float] = {
    "pers_pei": 2,
    "pers_ord": 2,
    "pers_revenue": 359.98,
    "min_price": 359.98,
    "diff_coef": 5.42,
    "wgh_pers_ord": 0.08,
    "std_price": 0.0,
    "diff_ord": 0.0,
    "reg_b": 2,
    "items_b": 16,
    "rev_coef": 0.168,
    "pei_coef": 0.182,
    "perc_return": 0.0,
    "perc_discount": 0.0,
    "first_b": 2,
    "last_b": 2,
}

PO_COLS: list[str] = ["offl_ord", "offl_pei"]
PO_ROW_DISPLAY: list[float] = [41.0, 17.0]


def _pi_raw_v1_v3() -> list[int]:
    out: list[int] = []
    for c in PI_COLS:
        d = PI_DISPLAY[c]
        mul = PERS_ITEM_MULTIPLIERS.get(c, 1)
        out.append(int(round(float(d) * mul)))
    return out


def _pi_row_v2_decoded() -> list[float]:
    """v2 в репозитории не делит на множители — в JSON уже «финальные» числа."""
    return [float(PI_DISPLAY[c]) for c in PI_COLS]


def _pui_raw_v1_v3() -> list[int]:
    out: list[int] = []
    for c in PUI_COLS:
        d = PUI_DISPLAY[c]
        mul = PERS_USER_ITEM_MULTIPLIERS.get(c, 1)
        out.append(int(round(float(d) * mul)))
    return out


def _pui_row_v2_decoded() -> list[float]:
    return [float(PUI_DISPLAY[c]) for c in PUI_COLS]


def _encode_pers_cols_v1() -> dict[str, bytes]:
    return {
        "pers_user_item": msgpack.packb(PUI_COLS, use_bin_type=False),
        "pers_item": msgpack.packb(PI_COLS, use_bin_type=False),
        "pers_offl": msgpack.packb(PO_COLS, use_bin_type=False),
    }


def _encode_pers_cols_v2() -> dict[str, bytes]:
    return {
        "pers_user_item": json.dumps(PUI_COLS).encode("utf-8"),
        "pers_item": json.dumps(PI_COLS).encode("utf-8"),
        "pers_offl": json.dumps(PO_COLS).encode("utf-8"),
    }


def _seed_user_cities_v1(r: redis.Redis) -> None:
    for uid, city, _ in SCENARIOS:
        r.hset("pers_user_city", str(uid), msgpack.packb([city], use_bin_type=False))


def _seed_user_cities_v2(r: redis.Redis) -> None:
    for uid, city, _ in SCENARIOS:
        r.hset(
            "pers_user_city",
            str(uid),
            json.dumps([city]).encode("utf-8"),
        )


def _seed_pers_user_item_v1(r: redis.Redis) -> None:
    row = _pui_raw_v1_v3()
    for uid, city, items in SCENARIOS:
        hkey = f"pers_user_item:{BRAND}:{uid}:{city}"
        for item_id in items:
            r.hset(hkey, str(item_id), msgpack.packb(row, use_bin_type=False))


def _seed_pers_user_item_v2(r: redis.Redis) -> None:
    row = _pui_row_v2_decoded()
    for uid, city, items in SCENARIOS:
        hkey = f"pers_user_item:{BRAND}:{uid}:{city}"
        for item_id in items:
            r.hset(
                hkey,
                str(item_id),
                json.dumps(row).encode("utf-8"),
            )


def _all_item_ids() -> list[int]:
    out: list[int] = []
    for *_, items in SCENARIOS:
        out.extend(items)
    return out


def _seed_pers_item_v1(r: redis.Redis) -> None:
    row = _pi_raw_v1_v3()
    hkey = f"pers_item:{BRAND}:-1"
    for item_id in _all_item_ids():
        r.hset(hkey, str(item_id), msgpack.packb(row, use_bin_type=False))


def _seed_pers_item_v2(r: redis.Redis) -> None:
    row = _pi_row_v2_decoded()
    hkey = f"pers_item:{BRAND}:-1"
    for item_id in _all_item_ids():
        r.hset(
            hkey,
            str(item_id),
            json.dumps(row).encode("utf-8"),
        )


def _seed_pers_offl_v1(r: redis.Redis) -> None:
    row = [int(x) for x in PO_ROW_DISPLAY]
    packed = msgpack.packb(row, use_bin_type=False)
    for uid, _, items in SCENARIOS:
        hkey = f"pers_offl:{uid}"
        for item_id in items:
            r.hset(hkey, str(item_id), packed)


def _seed_pers_offl_v2(r: redis.Redis) -> None:
    row = PO_ROW_DISPLAY
    raw = json.dumps(row).encode("utf-8")
    for uid, _, items in SCENARIOS:
        hkey = f"pers_offl:{uid}"
        for item_id in items:
            r.hset(hkey, str(item_id), raw)


def _seed_one(version: str, r_primary: redis.Redis) -> None:
    enc = {"v1": _encode_pers_cols_v1, "v2": _encode_pers_cols_v2, "v3": _encode_pers_cols_v1}
    uc = {"v1": _seed_user_cities_v1, "v2": _seed_user_cities_v2, "v3": _seed_user_cities_v1}
    pui = {
        "v1": _seed_pers_user_item_v1,
        "v2": _seed_pers_user_item_v2,
        "v3": _seed_pers_user_item_v1,
    }
    pi = {"v1": _seed_pers_item_v1, "v2": _seed_pers_item_v2, "v3": _seed_pers_item_v1}
    po = {"v1": _seed_pers_offl_v1, "v2": _seed_pers_offl_v2, "v3": _seed_pers_offl_v1}

    r_primary.hset("pers_cols", mapping=enc[version]())
    uc[version](r_primary)
    pui[version](r_primary)
    pi[version](r_primary)
    po[version](r_primary)


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed demo data for Feature Gateway")
    parser.add_argument("--version", choices=("v1", "v2", "v3"))
    parser.add_argument("--all", action="store_true", help="Засидировать все три порта")
    parser.add_argument("--flush", action="store_true", help="FLUSHDB перед записью")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Один порт для всех версий (локально один Redis); переопределяет --port-v1/2/3",
    )
    parser.add_argument("--port-v1", type=int, default=DEFAULT_PORTS["v1"])
    parser.add_argument("--port-v2", type=int, default=DEFAULT_PORTS["v2"])
    parser.add_argument("--port-v3", type=int, default=DEFAULT_PORTS["v3"])
    args = parser.parse_args()

    if args.port is not None:
        args.port_v1 = args.port_v2 = args.port_v3 = args.port

    if not args.version and not args.all:
        parser.error("Нужен --version v1|v2|v3 или --all")

    versions = ["v1", "v2", "v3"] if args.all else [args.version]  # type: ignore[list-item]

    port_by_version = {
        "v1": args.port_v1,
        "v2": args.port_v2,
        "v3": args.port_v3,
    }

    if (
        args.all
        and port_by_version["v1"] == port_by_version["v2"] == port_by_version["v3"]
    ):
        print(
            "Ошибка: --all с одним и тем же портом трижды перезапишет БД. "
            "Для одного Redis запустите один раз, например: "
            "python scripts/seed_demo_data.py --version v1 --flush",
            file=sys.stderr,
        )
        sys.exit(2)

    for v in versions:
        port = port_by_version[v]
        r = redis.Redis(host=args.host, port=port, db=0, decode_responses=False)
        if args.flush:
            r.flushdb()
            print(f"[{v}] FLUSHDB {args.host}:{port}")
        _seed_one(v, r)
        print(f"[{v}] OK — redis://{args.host}:{port}/0")


if __name__ == "__main__":
    try:
        main()
    except redis.ConnectionError as e:
        print(f"Redis недоступен: {e}", file=sys.stderr)
        sys.exit(1)
