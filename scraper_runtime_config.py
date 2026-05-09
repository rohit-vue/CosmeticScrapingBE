from __future__ import annotations

import json
import os
from typing import Iterable, TypeVar


T = TypeVar("T")


def _split_values(raw: str) -> list[str]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, list):
        values = [str(x).strip() for x in parsed]
    else:
        values = [
            part.strip()
            for part in raw.replace("\r", "\n").replace(",", "\n").split("\n")
        ]
    return [x for x in values if x]


def env_list(name: str, default: Iterable[str]) -> list[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return list(default)
    values = _split_values(raw)
    return values or list(default)


def env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, value)

