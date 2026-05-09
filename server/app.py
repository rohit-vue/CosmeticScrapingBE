from __future__ import annotations

import asyncio
import json
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse

from .registry import SCRAPERS
from .runs import RunManager
from .schemas import StartRunRequest, StartRunResponse, StopRunRequest
from .storage import RUNS_DIR


app = FastAPI(title="Cosmetic Scraper Orchestrator")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

manager = RunManager()


def _sse(payload: dict[str, object]) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=True)}\n\n"


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/scrapers")
def list_scrapers() -> list[dict[str, object]]:
    snap = manager.snapshot()
    return snap["scrapers"]  # type: ignore[return-value]


@app.post("/api/runs", response_model=StartRunResponse)
def start_run(body: StartRunRequest) -> StartRunResponse:
    try:
        run_id = manager.start_run(
            body.scraper_ids,
            keywords=body.keywords,
            countries=body.countries,
            target_suppliers=body.target_suppliers,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return StartRunResponse(run_id=run_id)


@app.get("/api/runs/{run_id}")
def get_run(run_id: str) -> dict[str, object]:
    run = manager.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return {
        "run_id": run.run_id,
        "status": run.status,
        "started_at": run.started_at,
        "ended_at": run.ended_at,
        "scraper_ids": run.scraper_ids,
        "combined_csv": str(run.combined_csv) if run.combined_csv else None,
    }


@app.post("/api/runs/{run_id}/stop")
def stop_run(run_id: str, body: StopRunRequest) -> dict[str, bool]:
    manager.stop_run(run_id, body.scraper_ids)
    return {"ok": True}


@app.get("/api/runs/{run_id}/combined.csv")
def combined_csv(run_id: str) -> FileResponse:
    # Serve combined CSV directly from the runs directory so historical
    # runs remain downloadable even after the manager forgets them.
    path = RUNS_DIR / run_id / "combined_suppliers.csv"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Combined CSV not found")
    return FileResponse(
        path=str(path),
        filename=f"combined_suppliers_{run_id[:8]}.csv",
        media_type="text/csv",
    )


@app.get("/api/runs/{run_id}/scrapers/{scraper_id}/cleaned.csv")
def scraper_cleaned_csv(run_id: str, scraper_id: str) -> FileResponse:
    scraper = SCRAPERS.get(scraper_id)
    if scraper is None:
        raise HTTPException(status_code=404, detail="Unknown scraper id")
    path = RUNS_DIR / run_id / scraper.cleaned_csv
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail="Cleaned CSV not available for this run",
        )
    return FileResponse(
        path=str(path),
        filename=f"{scraper.id}_cleaned_{run_id[:8]}.csv",
        media_type="text/csv",
    )


@app.get("/api/runs/{run_id}/scrapers/{scraper_id}/raw.csv")
def scraper_raw_csv(run_id: str, scraper_id: str) -> FileResponse:
    scraper = SCRAPERS.get(scraper_id)
    if scraper is None:
        raise HTTPException(status_code=404, detail="Unknown scraper id")
    path = RUNS_DIR / run_id / scraper.output_csv
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail="Raw CSV not available for this run",
        )
    return FileResponse(
        path=str(path),
        filename=f"{scraper.id}_raw_{run_id[:8]}.csv",
        media_type="text/csv",
    )


@app.get("/api/runs/{run_id}/scrapers/{scraper_id}/partial.csv")
def scraper_partial_csv(run_id: str, scraper_id: str) -> FileResponse:
    scraper = SCRAPERS.get(scraper_id)
    if scraper is None:
        raise HTTPException(status_code=404, detail="Unknown scraper id")
    path = RUNS_DIR / run_id / scraper.partial_csv
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail="Partial CSV not available for this run",
        )
    return FileResponse(
        path=str(path),
        filename=f"{scraper.id}_partial_{run_id[:8]}.csv",
        media_type="text/csv",
    )


@app.get("/api/runs/{run_id}/state")
async def stream_state(run_id: str) -> StreamingResponse:
    async def gen():
        snap = manager.snapshot()
        if snap.get("run_id") != run_id:
            yield _sse({"run_id": run_id, "run_status": "unknown", "scrapers": []})
            return
        terminal = {"succeeded", "failed", "stopped"}
        last_payload: str | None = None
        while True:
            snap = manager.snapshot()
            if snap.get("run_id") != run_id:
                yield _sse({"run_id": run_id, "run_status": "unknown", "scrapers": []})
                return
            payload = _sse(snap)
            if payload != last_payload:
                yield payload
                last_payload = payload
            if snap.get("run_status") in terminal:
                return
            await asyncio.sleep(1.0)

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/api/runs/{run_id}/scrapers/{scraper_id}/logs")
async def stream_logs(run_id: str, scraper_id: str) -> StreamingResponse:
    async def gen():
        for item in manager.logs.replay(run_id, scraper_id):
            yield _sse(item)
        q = manager.logs.subscribe(run_id, scraper_id)
        try:
            while True:
                ev = await q.get()
                yield _sse(ev.as_dict())
        finally:
            manager.logs.unsubscribe(run_id, scraper_id, q)

    return StreamingResponse(gen(), media_type="text/event-stream")

