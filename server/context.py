from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


ScraperStatus = Literal["idle", "queued", "running", "succeeded", "failed", "stopped"]


@dataclass
class RunSummary:
    id: str
    started_at: str
    ended_at: str | None = None
    duration_ms: int | None = None
    records_found: int | None = None
    error_message: str | None = None
    outcome: Literal["succeeded", "failed", "stopped"] | None = None


@dataclass
class ScraperState:
    id: str
    name: str
    domain: str
    script_file: str
    output_csv: str
    keywords: list[str]
    countries: list[str]
    status: ScraperStatus = "idle"
    progress: int = 0
    last_run: RunSummary | None = None
    recent_runs: list[RunSummary] = field(default_factory=list)


@dataclass
class RunRecord:
    run_id: str
    scraper_ids: list[str]
    started_at: str
    keywords: list[str] | None = None
    countries: list[str] | None = None
    target_suppliers: int | None = None
    ended_at: str | None = None
    status: Literal["running", "succeeded", "failed", "stopped"] = "running"
    combined_csv: Path | None = None

