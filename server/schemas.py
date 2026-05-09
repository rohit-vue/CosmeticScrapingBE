from __future__ import annotations

from pydantic import BaseModel, Field


class StartRunRequest(BaseModel):
    scraper_ids: list[str] = Field(min_length=1)
    keywords: list[str] | None = None
    countries: list[str] | None = None
    target_suppliers: int | None = Field(default=None, ge=1)


class StopRunRequest(BaseModel):
    scraper_ids: list[str] | None = None


class StartRunResponse(BaseModel):
    run_id: str

