from __future__ import annotations

from pathlib import Path

import pandas as pd

from .registry import SCRAPERS, ScraperDef


# Unified output schema. Every cleaned CSV (per-scraper and combined) is
# guaranteed to contain exactly these columns in this order.
COMBINED_COLUMNS = [
    "company_name",
    "website_url",
    "country",
    "email",
    "source_directory",
    "profile_url",
    "company_description",
]


def _empty_cleaned() -> pd.DataFrame:
    return pd.DataFrame(columns=COMBINED_COLUMNS)


def _read_raw(path: Path, sep: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, sep=sep, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _normalize(df: pd.DataFrame, source_directory: str) -> pd.DataFrame:
    if df.empty:
        return _empty_cleaned()

    out = df.copy()

    # Map known per-source aliases onto the unified schema.
    if "tradewheel_profile_url" in out.columns and "profile_url" not in out.columns:
        out["profile_url"] = out["tradewheel_profile_url"]

    for col in COMBINED_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[COMBINED_COLUMNS].copy()

    # Stamp source_directory consistently so combined output is always traceable.
    out["source_directory"] = source_directory

    for col in ("company_name", "website_url", "country", "email", "source_directory", "profile_url", "company_description"):
        out[col] = out[col].fillna("").astype(str).str.strip()

    email_lower = out["email"].str.lower()
    out = out[~email_lower.str.contains(
        r"example\.com|nobody@|cloudflare|notfound|blocked|error|copyright",
        regex=True,
        na=False,
    )]
    out = out[out["email"] != ""]
    out = out[out["company_name"] != ""]

    out["_name_norm"] = out["company_name"].str.lower()
    out = out.drop_duplicates(subset=["_name_norm"], keep="first").drop(columns=["_name_norm"])
    return out.reset_index(drop=True)


def clean_scraper_csv(scraper_id: str, run_path: Path) -> Path | None:
    """Read the scraper's raw CSV from `run_path` and write a cleaned CSV
    (unified schema, email-required, deduped by company name).

    Checks every archived CSV artifact for that scraper so enrichment/checkpoint
    files can be used when the phase-1 raw file has no emails yet.

    Returns the path to the cleaned CSV. The file is always written, even
    when there is nothing to clean — in that case it contains just headers.
    """
    scraper: ScraperDef | None = SCRAPERS.get(scraper_id)
    if scraper is None:
        return None
    cleaned = _empty_cleaned()
    for artifact_name in scraper.csv_artifacts:
        raw = _read_raw(run_path / artifact_name, scraper.default_sep)
        candidate = _normalize(raw, source_directory=scraper.id)
        if not candidate.empty:
            cleaned = candidate
            break
    out_path = run_path / scraper.cleaned_csv
    cleaned.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


def cleaned_record_count(scraper_id: str, run_path: Path) -> int | None:
    scraper = SCRAPERS.get(scraper_id)
    if scraper is None:
        return None
    path = run_path / scraper.cleaned_csv
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        return 0
    return int(len(df))


def merge_cleaned_outputs(run_id: str, run_path: Path, scraper_ids: list[str]) -> Path:
    """Concatenate every per-scraper cleaned CSV in `run_path` into a single
    `combined_suppliers.csv` (same unified schema). Cross-source duplicates by
    company_name are removed, keeping the first occurrence.
    """
    frames: list[pd.DataFrame] = []
    for sid in scraper_ids:
        scraper = SCRAPERS.get(sid)
        if scraper is None:
            continue
        cleaned_path = run_path / scraper.cleaned_csv
        if not cleaned_path.exists():
            continue
        try:
            df = pd.read_csv(cleaned_path, encoding="utf-8-sig")
        except pd.errors.EmptyDataError:
            continue
        if df.empty:
            continue
        for col in COMBINED_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        frames.append(df[COMBINED_COLUMNS])

    if frames:
        merged = pd.concat(frames, ignore_index=True)
        merged["company_name"] = merged["company_name"].fillna("").astype(str).str.strip()
        merged["_name_norm"] = merged["company_name"].str.lower()
        merged = merged.drop_duplicates(subset=["_name_norm"], keep="first").drop(columns=["_name_norm"])
    else:
        merged = _empty_cleaned()

    combined_path = run_path / "combined_suppliers.csv"
    merged.to_csv(combined_path, index=False, encoding="utf-8-sig")
    return combined_path
