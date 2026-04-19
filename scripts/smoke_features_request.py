#!/usr/bin/env python3
"""Простой POST к /api/v1/features (без зависимости от httpx).

  FEATURE_GATEWAY_URL=http://127.0.0.1:8000/api/v1/features python scripts/smoke_features_request.py
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_URL = "http://127.0.0.1:8000/api/v1/features"


def main() -> None:
    url = os.environ.get("FEATURE_GATEWAY_URL", DEFAULT_URL)
    body_path = Path(__file__).resolve().parent / "demo_request.json"
    body = json.loads(body_path.read_text(encoding="utf-8"))

    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            out = json.loads(raw)
            print(json.dumps(out, ensure_ascii=False, indent=2))
    except urllib.error.HTTPError as e:
        print(e.read().decode("utf-8", errors="replace"), file=sys.stderr)
        sys.exit(e.code)


if __name__ == "__main__":
    main()
