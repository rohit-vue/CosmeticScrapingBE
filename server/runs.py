from __future__ import annotations

import os
import re
import json
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import monotonic
from uuid import uuid4

from .context import RunRecord, RunSummary, ScraperState, utc_now_iso
from .logs import LogHub
from .merge import clean_scraper_csv, cleaned_record_count, merge_cleaned_outputs
from .registry import SCRAPERS
from .storage import run_dir, write_run_meta


# Patterns for low-signal scraper output that just floods the UI.
# Each pattern is matched with re.search against the full message.
# Override with SCRAPER_LOG_VERBOSE=1 to disable filtering.
_NOISE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^\s*$"),
    re.compile(r"^[\s=\-*~#_.]+$"),
    re.compile(r"Page\s+\d+\s*:\s*(?:Not\s+found|Blocked|No\s+companies)", re.I),
    re.compile(r"Failed\s+page\s+load", re.I),
    re.compile(r"Failed\s+to\s+fetch", re.I),
    re.compile(r"\b(?:HTTP|Status(?:\s*Code)?)\s*[:=]?\s*(?:404|403|429|5\d\d)\b", re.I),
    re.compile(r"\b40[34]\s+(?:Not\s+Found|Forbidden)\b", re.I),
    re.compile(r"Connection\s+(?:error|reset|aborted|refused)", re.I),
    re.compile(r"\bSSL\b.*\b(?:error|handshake)\b", re.I),
    re.compile(r"\bRetry(?:ing)?\b.*\battempt\b", re.I),
    re.compile(r"^\s*\.+\s*$"),
)

_LEVEL_HINTS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\btraceback\b", re.I), "error"),
    (re.compile(r"\[ERROR\]|\bexception\b", re.I), "error"),
    (re.compile(r"\[WARN(?:ING)?\]|\bwarning\b", re.I), "warn"),
    (re.compile(r"\[DONE\]|\[SUCCESS\]|\bcompleted\b", re.I), "success"),
)

_MAX_LOG_LINE_LEN = 800


def _classify_level(message: str) -> str:
    for pat, lvl in _LEVEL_HINTS:
        if pat.search(message):
            return lvl
    return "info"


def _is_noise(message: str) -> bool:
    for pat in _NOISE_PATTERNS:
        if pat.search(message):
            return True
    return False


def _shrink(message: str) -> str:
    if len(message) <= _MAX_LOG_LINE_LEN:
        return message
    return message[: _MAX_LOG_LINE_LEN - 12].rstrip() + "...[truncated]"


class RunManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.logs = LogHub(max_per_stream=1000)
        self.states: dict[str, ScraperState] = {
            s.id: ScraperState(
                id=s.id,
                name=s.name,
                domain=s.domain,
                script_file=s.script_file,
                output_csv=s.output_csv,
                keywords=list(s.keywords),
                countries=list(s.countries),
            )
            for s in SCRAPERS.values()
        }
        self.active_run: RunRecord | None = None
        self._active_procs: dict[str, subprocess.Popen[str]] = {}
        self._executor = ThreadPoolExecutor(max_workers=max(1, len(SCRAPERS)))

    def list_scrapers(self) -> list[ScraperState]:
        with self._lock:
            return [self._copy_state(s) for s in self.states.values()]

    def get_run(self, run_id: str) -> RunRecord | None:
        with self._lock:
            if self.active_run and self.active_run.run_id == run_id:
                return self.active_run
            return None

    def start_run(
        self,
        scraper_ids: list[str],
        *,
        keywords: list[str] | None = None,
        countries: list[str] | None = None,
        target_suppliers: int | None = None,
    ) -> str:
        run_keywords = self._clean_list(keywords)
        run_countries = self._clean_list(countries)
        run_target = target_suppliers if target_suppliers and target_suppliers > 0 else None
        with self._lock:
            ids = [sid for sid in scraper_ids if sid in SCRAPERS]
            if not ids:
                raise RuntimeError("No valid scraper ids provided.")
            if self.active_run and self.active_run.status == "running":
                run_id = self.active_run.run_id
                active_ids = set(self.active_run.scraper_ids)
                ids = [
                    sid for sid in ids
                    if sid not in active_ids and self.states[sid].status not in {"queued", "running"}
                ]
                if not ids:
                    return run_id
                self.active_run.scraper_ids.extend(ids)
                if run_keywords:
                    self.active_run.keywords = run_keywords
                if run_countries:
                    self.active_run.countries = run_countries
                if run_target:
                    self.active_run.target_suppliers = run_target
                for sid in ids:
                    if run_keywords:
                        self.states[sid].keywords = list(run_keywords)
                    if run_countries:
                        self.states[sid].countries = list(run_countries)
                    self.states[sid].status = "queued"
                    self.states[sid].progress = 0
                self._executor.submit(
                    self._execute_run,
                    run_id,
                    ids,
                    run_keywords,
                    run_countries,
                    run_target,
                )
                return run_id
            run_id = str(uuid4())
            self.active_run = RunRecord(
                run_id=run_id,
                scraper_ids=ids,
                started_at=utc_now_iso(),
                keywords=run_keywords,
                countries=run_countries,
                target_suppliers=run_target,
            )
            for sid in ids:
                if run_keywords:
                    self.states[sid].keywords = list(run_keywords)
                if run_countries:
                    self.states[sid].countries = list(run_countries)
                self.states[sid].status = "queued"
                self.states[sid].progress = 0
            self._executor.submit(
                self._execute_run,
                run_id,
                ids,
                run_keywords,
                run_countries,
                run_target,
            )
            return run_id

    def stop_run(self, run_id: str, scraper_ids: list[str] | None = None) -> None:
        with self._lock:
            if not self.active_run or self.active_run.run_id != run_id:
                return
            target_ids = scraper_ids or list(self._active_procs.keys())
            for sid in target_ids:
                proc = self._active_procs.get(sid)
                if proc and proc.poll() is None:
                    proc.terminate()
                    self.states[sid].status = "stopped"
                    self.logs.emit(run_id, sid, "warn", "Stop requested by user")

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            run = self.active_run
            return {
                "run_id": run.run_id if run else None,
                "run_status": run.status if run else None,
                "run_scraper_ids": list(run.scraper_ids) if run else [],
                "scrapers": [self._state_to_payload(s) for s in self.states.values()],
            }

    def _execute_run(
        self,
        run_id: str,
        scraper_ids: list[str],
        keywords: list[str] | None = None,
        countries: list[str] | None = None,
        target_suppliers: int | None = None,
    ) -> None:
        root = Path(__file__).resolve().parent.parent
        output_dir = run_dir(run_id)
        failures = 0
        stopped = 0
        try:
            max_parallel = max(1, int(os.getenv("SCRAPER_MAX_PARALLEL", "3")))
        except ValueError:
            max_parallel = 3

        def task(scraper_id: str) -> tuple[str, RunSummary]:
            scraper = SCRAPERS[scraper_id]
            started = utc_now_iso()
            t0 = monotonic()
            with self._lock:
                self.states[scraper_id].status = "running"
                self.states[scraper_id].progress = 5

            # Use same interpreter as the API process (venv), not bare "python" on PATH.
            # `-X utf8` forces UTF-8 mode for stdio so scrapers can print Unicode (e.g. arrows, accents)
            # on Windows consoles that default to cp1252.
            cmd = [sys.executable, "-X", "utf8", "-u", scraper.script_file]
            self.logs.emit(run_id, scraper_id, "info", f"Running {' '.join(cmd)}")
            child_env = {
                **os.environ,
                "PYTHONIOENCODING": "utf-8",
                "PYTHONUTF8": "1",
                "PYTHONUNBUFFERED": "1",
            }
            if keywords:
                child_env["SCRAPER_KEYWORDS"] = json.dumps(keywords, ensure_ascii=True)
                self.logs.emit(run_id, scraper_id, "info", f"Using manual keywords: {', '.join(keywords)}")
            if countries:
                child_env["SCRAPER_COUNTRIES"] = json.dumps(countries, ensure_ascii=True)
                self.logs.emit(run_id, scraper_id, "info", f"Using manual countries: {', '.join(countries)}")
            if target_suppliers:
                child_env["SCRAPER_TARGET_SUPPLIERS"] = str(target_suppliers)
                self.logs.emit(run_id, scraper_id, "info", f"Using manual target suppliers: {target_suppliers}")
            proc = subprocess.Popen(
                cmd,
                cwd=str(root),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=child_env,
            )
            with self._lock:
                self._active_procs[scraper_id] = proc

            verbose = os.getenv("SCRAPER_LOG_VERBOSE", "").strip().lower() in {"1", "true", "yes", "on"}
            suppressed = 0
            last_summary_at = monotonic()
            assert proc.stdout is not None
            for line in proc.stdout:
                msg = _shrink(line.rstrip())
                if not verbose and _is_noise(msg):
                    suppressed += 1
                    # Periodic heartbeat so the UI knows the scraper is still alive.
                    if monotonic() - last_summary_at >= 30 and suppressed:
                        self.logs.emit(
                            run_id,
                            scraper_id,
                            "info",
                            f"(suppressed {suppressed} low-signal lines)",
                        )
                        suppressed = 0
                        last_summary_at = monotonic()
                    continue
                self.logs.emit(run_id, scraper_id, _classify_level(msg), msg)
            if suppressed:
                self.logs.emit(
                    run_id,
                    scraper_id,
                    "info",
                    f"(suppressed {suppressed} low-signal lines total)",
                )

            code = proc.wait()
            with self._lock:
                self._active_procs.pop(scraper_id, None)
            elapsed = int((monotonic() - t0) * 1000)

            for artifact_name in scraper.csv_artifacts:
                artifact_src = root / artifact_name
                if artifact_src.exists():
                    artifact_src.replace(output_dir / artifact_name)
            # Rebuild cleaned CSV in the run folder so every scraper uses a unified schema.
            cleaned_path: Path | None = None
            cleaned_count: int | None = None
            try:
                cleaned_path = clean_scraper_csv(scraper_id, output_dir)
                cleaned_count = cleaned_record_count(scraper_id, output_dir)
            except Exception as exc:  # pragma: no cover - defensive
                self.logs.emit(
                    run_id,
                    scraper_id,
                    "warn",
                    f"Failed to build cleaned CSV: {exc}",
                )

            summary = RunSummary(id=run_id, started_at=started, ended_at=utc_now_iso(), duration_ms=elapsed)
            summary.records_found = cleaned_count
            if code == 0:
                summary.outcome = "succeeded"
                if cleaned_path is not None:
                    self.logs.emit(
                        run_id,
                        scraper_id,
                        "info",
                        f"Cleaned CSV written: {cleaned_path.name} ({cleaned_count or 0} rows)",
                    )
                self.logs.emit(run_id, scraper_id, "success", f"{scraper.name} completed")
            elif self.states[scraper_id].status == "stopped":
                summary.outcome = "stopped"
                if cleaned_path is not None:
                    self.logs.emit(
                        run_id,
                        scraper_id,
                        "info",
                        f"Cleaned CSV written from partial data: {cleaned_path.name} ({cleaned_count or 0} rows)",
                    )
                self.logs.emit(run_id, scraper_id, "warn", f"{scraper.name} stopped")
            else:
                summary.outcome = "failed"
                summary.error_message = f"Exit code {code}"
                if cleaned_path is not None:
                    self.logs.emit(
                        run_id,
                        scraper_id,
                        "info",
                        f"Cleaned CSV written from partial data: {cleaned_path.name} ({cleaned_count or 0} rows)",
                    )
                self.logs.emit(run_id, scraper_id, "error", f"{scraper.name} failed with code {code}")

            # Update state and last/recent_runs atomically here so the
            # frontend sees a consistent terminal status + records_found in the
            # very same SSE snapshot, which is what the auto-download watcher
            # needs to fire once-per-scraper-finish.
            with self._lock:
                state = self.states[scraper_id]
                if summary.outcome == "succeeded":
                    state.status = "succeeded"
                    state.progress = 100
                elif summary.outcome == "stopped":
                    state.status = "stopped"
                    state.progress = 0
                else:
                    state.status = "failed"
                    state.progress = 0
                state.last_run = summary
                state.recent_runs = [summary, *state.recent_runs][:8]
            return scraper_id, summary

        with ThreadPoolExecutor(max_workers=min(max_parallel, len(scraper_ids))) as pool:
            futures = [pool.submit(task, sid) for sid in scraper_ids]
            for fut in futures:
                _sid, summary = fut.result()
                if summary.outcome == "failed":
                    failures += 1
                if summary.outcome == "stopped":
                    stopped += 1

        with self._lock:
            active_ids = (
                list(self.active_run.scraper_ids)
                if self.active_run and self.active_run.run_id == run_id
                else list(scraper_ids)
            )

        combined = merge_cleaned_outputs(run_id=run_id, run_path=output_dir, scraper_ids=active_ids)
        with self._lock:
            if self.active_run and self.active_run.run_id == run_id:
                self.active_run.combined_csv = combined
                active_ids = list(self.active_run.scraper_ids)
                still_active = any(
                    self.states[sid].status in {"queued", "running"}
                    for sid in active_ids
                    if sid in self.states
                )
                if still_active:
                    return
                failures = sum(
                    1
                    for sid in active_ids
                    if sid in self.states and self.states[sid].last_run and self.states[sid].last_run.outcome == "failed"
                )
                stopped = sum(
                    1
                    for sid in active_ids
                    if sid in self.states and self.states[sid].last_run and self.states[sid].last_run.outcome == "stopped"
                )
                self.active_run.ended_at = utc_now_iso()
                if failures:
                    self.active_run.status = "failed"
                elif stopped and stopped == len(active_ids):
                    self.active_run.status = "stopped"
                else:
                    self.active_run.status = "succeeded"
                write_run_meta(
                    run_id,
                    {
                        "run_id": run_id,
                        "status": self.active_run.status,
                        "started_at": self.active_run.started_at,
                        "ended_at": self.active_run.ended_at,
                        "scraper_ids": active_ids,
                        "keywords": self.active_run.keywords,
                        "countries": self.active_run.countries,
                        "target_suppliers": self.active_run.target_suppliers,
                        "combined_csv": str(combined),
                    },
                )

    @staticmethod
    def _clean_list(values: list[str] | None) -> list[str] | None:
        if not values:
            return None
        cleaned = [str(v).strip() for v in values if str(v).strip()]
        return cleaned or None

    @staticmethod
    def _copy_state(s: ScraperState) -> ScraperState:
        return ScraperState(
            id=s.id,
            name=s.name,
            domain=s.domain,
            script_file=s.script_file,
            output_csv=s.output_csv,
            keywords=list(s.keywords),
            countries=list(s.countries),
            status=s.status,
            progress=s.progress,
            last_run=s.last_run,
            recent_runs=list(s.recent_runs),
        )

    @staticmethod
    def _state_to_payload(s: ScraperState) -> dict[str, object]:
        def to_run(x: RunSummary) -> dict[str, object]:
            return {
                "id": x.id,
                "startedAt": x.started_at,
                "endedAt": x.ended_at,
                "durationMs": x.duration_ms,
                "recordsFound": x.records_found,
                "errorMessage": x.error_message,
                "outcome": x.outcome,
            }

        cleaned_csv = SCRAPERS[s.id].cleaned_csv if s.id in SCRAPERS else ""
        return {
            "id": s.id,
            "name": s.name,
            "domain": s.domain,
            "scriptFile": s.script_file,
            "outputCsv": s.output_csv,
            "cleanedCsv": cleaned_csv,
            "keywords": s.keywords,
            "countries": s.countries,
            "status": s.status,
            "progress": s.progress,
            "lastRun": to_run(s.last_run) if s.last_run else None,
            "recentRuns": [to_run(r) for r in s.recent_runs],
        }

