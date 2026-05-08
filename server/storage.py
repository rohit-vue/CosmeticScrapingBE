from __future__ import annotations

import json
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent.parent
RUNS_DIR = BASE_DIR / "runs"


def ensure_runs_dir() -> Path:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    return RUNS_DIR


def run_dir(run_id: str) -> Path:
    root = ensure_runs_dir() / run_id
    root.mkdir(parents=True, exist_ok=True)
    return root


def write_run_meta(run_id: str, payload: dict[str, Any]) -> None:
    path = run_dir(run_id) / "run.json"
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

