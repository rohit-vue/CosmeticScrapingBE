from __future__ import annotations

import csv
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, is_dataclass
from typing import Any

from openai import OpenAI


# AI filtering is disabled; do not append AI result columns to generated CSVs.
# AI_RESULT_FIELDS = ["is_target_supplier", "confidence", "ai_reason", "ai_target_keywords"]
AI_RESULT_FIELDS: list[str] = []


def load_env_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


load_env_file()


def normalize_keywords(keywords: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for keyword in keywords:
        clean = re.sub(r"[-_]+", " ", (keyword or "").strip())
        clean = re.sub(r"\s+", " ", clean)
        if clean and clean.lower() not in seen:
            seen.add(clean.lower())
            normalized.append(clean)
    return normalized


def target_signature(keywords: list[str]) -> str:
    return " | ".join(normalize_keywords(keywords)).lower()


def record_to_dict(record: Any) -> dict[str, Any]:
    if is_dataclass(record):
        return asdict(record)
    return dict(record)


def set_record_field(record: Any, field: str, value: Any) -> None:
    if isinstance(record, dict):
        record[field] = value
    else:
        setattr(record, field, value)


def row_key(row: dict[str, Any]) -> str:
    profile_url = str(row.get("profile_url") or "").strip().lower().rstrip("/")
    if profile_url:
        return f"profile:{profile_url}"
    website = str(row.get("website_url") or "").strip().lower().rstrip("/")
    name = re.sub(r"\s+", " ", str(row.get("company_name") or "").strip().lower())
    country = str(row.get("country") or "").strip().lower()
    return f"name:{name}|{country}|{website}"


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def parse_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def create_client(api_key: str = "") -> OpenAI:
    key = (api_key or os.getenv("OPENAI_API_KEY", "")).strip()
    if not key:
        raise ValueError("Set OPENAI_API_KEY in .env or environment")
    return OpenAI(api_key=key)


def create_system_prompt(keywords: list[str]) -> str:
    keyword_lines = "\n".join(f"- {keyword}" for keyword in normalize_keywords(keywords))
    return f"""You are a supply chain analyst specializing in cosmetic packaging suppliers.
Your task: determine if a company manufactures, supplies, distributes, or wholesales products matching AT LEAST ONE target keyword.

Target keywords:
{keyword_lines}

The company IS relevant if there is clear evidence that it supplies products matching one or more target keywords.
The company is NOT relevant if it only supplies unrelated products, raw materials, services, equipment, or generic packaging with no clear match to any target keyword.

Pass the company if it clearly matches at least one target keyword. Be strict. When in doubt, mark as NOT relevant."""


def create_batch_prompt(rows: list[dict[str, Any]], keywords: list[str]) -> str:
    keyword_lines = "\n".join(f"- {keyword}" for keyword in normalize_keywords(keywords))
    companies_text = []
    for idx, row in enumerate(rows, 1):
        description = str(row.get("company_description") or row.get("description") or "")[:500]
        classification = str(row.get("kompass_classification") or row.get("classification") or "")[:500]
        companies_text.append(f"""
[{idx}] {row.get('company_name', 'Unknown')} | {row.get('country', 'Unknown')}
Description: {description if description else 'N/A'}
Classification: {classification if classification else 'N/A'}
Website: {row.get('website_url', 'N/A')}
Profile: {row.get('profile_url', 'N/A')}
""")

    return f"""Analyze these {len(rows)} companies. For each, determine if it clearly matches AT LEAST ONE target keyword.

Target keywords:
{keyword_lines}

Respond ONLY with valid JSON in this exact format:
{{"results":[
  {{"idx":1,"name":"Company","relevant":true/false,"confidence":0.0-1.0,"reason":"Brief evidence"}}
]}}

Rules:
- relevant: true if there is clear evidence for at least one target keyword
- relevant: false if the company only matches unrelated products or generic packaging
- confidence: 0.8+ for a clear target-keyword supplier, 0.5-0.7 for likely, <0.5 for unclear
- reason: max 150 chars, mention the matched keyword/product evidence when relevant

Companies:
{"".join(companies_text)}"""


def analyze_batch(
    client: OpenAI,
    rows: list[dict[str, Any]],
    keywords: list[str],
    batch_num: int,
    total_batches: int,
    model: str,
    log_prefix: str,
) -> list[dict[str, Any]]:
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": create_system_prompt(keywords)},
                {"role": "user", "content": create_batch_prompt(rows, keywords)},
            ],
            temperature=0.1,
            max_tokens=2000,
            response_format={"type": "json_object"},
        )
        data = json.loads(response.choices[0].message.content or "{}")
        results = data.get("results", [])
        relevant_count = sum(1 for result in results if parse_bool(result.get("relevant")))
        print(f"  [{log_prefix} AI] Batch {batch_num}/{total_batches}: {relevant_count}/{len(results)} relevant")
        return results
    except Exception as exc:
        print(f"  [{log_prefix} AI] Batch {batch_num}/{total_batches} ERROR: {exc}")
        return [
            {
                "idx": i + 1,
                "name": row.get("company_name", ""),
                "relevant": False,
                "confidence": 0,
                "reason": f"Error: {str(exc)[:100]}",
            }
            for i, row in enumerate(rows)
        ]


def read_csv(path: str) -> list[dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def write_csv(path: str, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows({field: row.get(field, "") for field in fieldnames} for row in rows)


def load_checkpoint(path: str, keywords: list[str]) -> dict[str, dict[str, str]]:
    signature = target_signature(keywords)
    return {
        row_key(row): row
        for row in read_csv(path)
        if row_key(row) and str(row.get("ai_target_keywords") or "").strip().lower() == signature
    }


def apply_ai_filter_to_records(
    records: list[Any],
    *,
    keywords: list[str],
    source_name: str,
    verified_csv: str,
    rejected_csv: str,
    checkpoint_csv: str,
    min_confidence: float | None = None,
    batch_size: int | None = None,
    concurrent: int | None = None,
    model: str | None = None,
    api_key: str = "",
) -> list[Any]:
    if not records:
        return records

    min_confidence = float(min_confidence if min_confidence is not None else os.getenv("AI_MIN_CONFIDENCE", "0.6"))
    batch_size = int(batch_size if batch_size is not None else os.getenv("AI_BATCH_SIZE", "10"))
    concurrent = int(concurrent if concurrent is not None else os.getenv("AI_CONCURRENT", "5"))
    model = model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    signature = target_signature(keywords)
    fieldnames = list(record_to_dict(records[0]).keys())
    for field in AI_RESULT_FIELDS:
        if field not in fieldnames:
            fieldnames.append(field)

    rows = [record_to_dict(record) for record in records]
    checkpoint = load_checkpoint(checkpoint_csv, keywords)
    processed: list[dict[str, Any]] = []
    unprocessed_pairs: list[tuple[Any, dict[str, Any]]] = []

    for record, row in zip(records, rows):
        existing = checkpoint.get(row_key(row))
        if existing:
            for field in AI_RESULT_FIELDS:
                set_record_field(record, field, existing.get(field, ""))
                row[field] = existing.get(field, "")
            processed.append(row)
        else:
            unprocessed_pairs.append((record, row))

    print(f"\n[{source_name} AI] Target keywords: {', '.join(normalize_keywords(keywords))}")
    print(f"[{source_name} AI] Already processed: {len(processed)} | To process: {len(unprocessed_pairs)}")

    new_rows: list[dict[str, Any]] = []
    if unprocessed_pairs:
        client = create_client(api_key)
        batches = [
            unprocessed_pairs[i:i + batch_size]
            for i in range(0, len(unprocessed_pairs), batch_size)
        ]
        with ThreadPoolExecutor(max_workers=concurrent) as executor:
            futures = {
                executor.submit(
                    analyze_batch,
                    client,
                    [row for _, row in batch],
                    keywords,
                    idx + 1,
                    len(batches),
                    model,
                    source_name,
                ): batch
                for idx, batch in enumerate(batches)
            }
            for future in as_completed(futures):
                batch = futures[future]
                results = future.result()
                for fallback_idx, result in enumerate(results):
                    source_idx = int(parse_float(result.get("idx"), fallback_idx + 1)) - 1
                    record, row = batch[source_idx] if 0 <= source_idx < len(batch) else batch[fallback_idx]
                    row["is_target_supplier"] = parse_bool(result.get("relevant"))
                    row["confidence"] = parse_float(result.get("confidence"), 0.0)
                    row["ai_reason"] = result.get("reason", "")
                    row["ai_target_keywords"] = signature
                    for field in AI_RESULT_FIELDS:
                        set_record_field(record, field, row[field])
                    new_rows.append(row)
                write_csv(checkpoint_csv, processed + new_rows, fieldnames)

    all_rows = processed + new_rows
    row_by_key = {row_key(row): row for row in all_rows}
    verified_records: list[Any] = []
    verified_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    previous_verified = load_checkpoint(verified_csv, keywords)

    for record in records:
        row = row_by_key.get(row_key(record_to_dict(record)))
        if not row:
            continue
        is_verified = parse_bool(row.get("is_target_supplier")) and parse_float(row.get("confidence")) >= min_confidence
        if is_verified:
            previous = previous_verified.get(row_key(row))
            if previous and previous.get("email") and not row.get("email"):
                row["email"] = previous["email"]
                set_record_field(record, "email", previous["email"])
            verified_records.append(record)
            verified_rows.append(record_to_dict(record) | {field: row.get(field, "") for field in AI_RESULT_FIELDS})
        else:
            rejected_rows.append(row)

    verified_rows.sort(key=lambda row: parse_float(row.get("confidence")), reverse=True)
    write_csv(verified_csv, verified_rows, fieldnames)
    write_csv(rejected_csv, rejected_rows, fieldnames)
    print(f"[{source_name} AI] VERIFIED: {len(verified_records)} -> {verified_csv}")
    print(f"[{source_name} AI] REJECTED: {len(rejected_rows)} -> {rejected_csv}")
    return verified_records
