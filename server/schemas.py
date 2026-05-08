from __future__ import annotations

from pydantic import BaseModel, Field


class StartRunRequest(BaseModel):
    scraper_ids: list[str] = Field(min_length=1)


class StopRunRequest(BaseModel):
    scraper_ids: list[str] | None = None


class StartRunResponse(BaseModel):
    run_id: str

