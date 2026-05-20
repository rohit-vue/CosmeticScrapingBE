"""
Tradewheel supplier scraper - Optimized with login enrichment, email sanitization, and cleaned CSV.
Phase 3 upgraded with requests-based email enrichment (same as EC21/Made-in-China).
"""

from __future__ import annotations

import random
import re
import os
import time
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from dataclasses import dataclass, asdict
from html import unescape
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse

import pandas as pd
import requests
# AI filtering is currently disabled so the scraper jumps straight to email enrichment.
# from ai_supplier_filter import apply_ai_filter_to_records
from proxy_service import (
    ProxyEndpoint,
    ProxyExhaustedError,
    create_proxy_pool,
    fetch_with_proxy_rotation,
    goto_with_rotation,
)
from scraper_runtime_config import env_int, env_list
from scrapling.fetchers import StealthyFetcher

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None
    PlaywrightTimeoutError = Exception


KEYWORDS = [
    # TUBES
    "cosmetic tube",
    "squeeze tube cosmetic",
    "laminated tube cosmetic",
    "plastic cosmetic tube",
    "hand cream tube",
    "lip balm tube",
    "lotion tube packaging",
    "bb cream tube",
    
    # JARS & CONTAINERS
    "cosmetic jar",
    "cream jar packaging",
    "skincare jar supplier",
    "glass cosmetic jar",
    "airless jar cosmetic",
    
    # BOTTLES
    "cosmetic bottle supplier",
    "lotion bottle packaging",
    "serum bottle cosmetic",
    "airless bottle cosmetic",
    "pump bottle cosmetic",
    
    # PUMPS & DISPENSERS
    "lotion pump dispenser",
    "airless pump bottle",
    "cosmetic pump packaging",
    "foam pump cosmetic",
    
    # CAPS & CLOSURES
    "cosmetic cap supplier",
    "jar cap packaging",
    "bottle cap cosmetic",
    "flip top cap cosmetic",
    "disc top cap",
    "plastic closure cosmetic",
    "PP cap supplier",
    
    # GENERAL PACKAGING
    "cosmetic packaging supplier",
    "skincare packaging manufacturer",
    "beauty packaging supplier",
    "primary packaging cosmetics",
    "cosmetic packaging OEM",
]

COUNTRIES = [
    "China",
    "South Korea",
    "Taiwan",
    "Japan",
    "Vietnam",
    "Thailand",
    "Singapore",
    "Malaysia",
    "Hong Kong",
    "Ukraine",
    "Poland",
    "Czech Republic",
    "Hungary",
    "Romania",
    "Bulgaria",
    "Belarus",
    "Serbia",
    "Croatia",
    "Slovakia",
    "Slovenia",
    "Lithuania",
    "Latvia",
    "Turkey",
]

SOURCE_DIRECTORY = "Tradewheel"
BASE_DOMAIN = "https://www.tradewheel.com"
MAX_PAGES_PER_QUERY = 50
MIN_DELAY_SECONDS = 1
MAX_DELAY_SECONDS = 7
OUTPUT_CSV = "tradewheel_suppliers_raw.csv"
CLEANED_CSV = "tradewheel_suppliers_cleaned.csv"
PARTIAL_OUTPUT_CSV = "tradewheel_suppliers_partial.csv"
PHASE2_RESUME_STATE_FILE = "tradewheel_phase2_resume_state.txt"
TARGET_SUPPLIERS = 5000
AUTOSAVE_EVERY_NEW_RECORDS = 10

ENABLE_LOGGED_IN_ENRICHMENT = True
PAUSE_FOR_MANUAL_LOGIN = True
AUTH_BROWSER_PROFILE_DIR = ".tradewheel_auth_profile"
AUTH_BROWSER_HEADLESS = False
MAX_LOGGED_IN_ENRICHMENTS = 0
PLAYWRIGHT_AUTH_CHANNEL = "chrome"
PLAYWRIGHT_AUTH_EXTRA_ARGS = ["--disable-blink-features=AutomationControlled"]
TRADEWHEEL_EMAIL_ENV = "TRADEWHEEL_EMAIL"
TRADEWHEEL_PASSWORD_ENV = "TRADEWHEEL_PASSWORD"
AUTH_CLOUDFLARE_WAIT_SECONDS = env_int("TRADEWHEEL_AUTH_CF_WAIT_SECONDS", 600)
AUTH_CLOUDFLARE_POLL_SECONDS = env_int("TRADEWHEEL_AUTH_CF_POLL_SECONDS", 3)
AUTH_CLOUDFLARE_RELOAD_ATTEMPTS = env_int("TRADEWHEEL_AUTH_CF_RELOAD_ATTEMPTS", 2)
AUTH_USE_PROXY = os.getenv("TRADEWHEEL_AUTH_USE_PROXY", "").strip().lower() in {"1", "true", "yes", "on"}
AUTH_PROFILE_MIN_DELAY_SECONDS = env_int("TRADEWHEEL_AUTH_PROFILE_MIN_DELAY_SECONDS", 5)
AUTH_PROFILE_MAX_DELAY_SECONDS = env_int("TRADEWHEEL_AUTH_PROFILE_MAX_DELAY_SECONDS", 10)
AUTH_SHOW_MIN_JITTER_MS = env_int("TRADEWHEEL_AUTH_SHOW_MIN_JITTER_MS", 900)
AUTH_SHOW_MAX_JITTER_MS = env_int("TRADEWHEEL_AUTH_SHOW_MAX_JITTER_MS", 3500)
AUTH_COOLDOWN_EVERY_MIN = env_int("TRADEWHEEL_AUTH_COOLDOWN_EVERY_MIN", 0)
AUTH_COOLDOWN_EVERY_MAX = env_int("TRADEWHEEL_AUTH_COOLDOWN_EVERY_MAX", 0)
AUTH_COOLDOWN_MIN_SECONDS = env_int("TRADEWHEEL_AUTH_COOLDOWN_MIN_SECONDS", 0)
AUTH_COOLDOWN_MAX_SECONDS = env_int("TRADEWHEEL_AUTH_COOLDOWN_MAX_SECONDS", 0)
AUTH_REVEAL_EMAIL = os.getenv("TRADEWHEEL_AUTH_REVEAL_EMAIL", "").strip().lower() in {"1", "true", "yes", "on"}
AUTH_VISIT_PROFILE_SUBPAGES = os.getenv("TRADEWHEEL_AUTH_VISIT_PROFILE_SUBPAGES", "false").strip().lower() in {"1", "true", "yes", "on"}
AUTH_SUBPAGE_MIN_DELAY_SECONDS = env_int("TRADEWHEEL_AUTH_SUBPAGE_MIN_DELAY_SECONDS", 4)
AUTH_SUBPAGE_MAX_DELAY_SECONDS = env_int("TRADEWHEEL_AUTH_SUBPAGE_MAX_DELAY_SECONDS", 12)
AUTH_MAX_CONSECUTIVE_ACCESS_FAILURES = env_int("TRADEWHEEL_AUTH_MAX_CONSECUTIVE_ACCESS_FAILURES", 1)
AUTH_STOP_ON_CLOUDFLARE = os.getenv("TRADEWHEEL_AUTH_STOP_ON_CLOUDFLARE", "true").strip().lower() not in {"0", "false", "no", "off"}

# SEARCH PAGES (Cloudflare-sensitive)
USE_BROWSER_FOR_SEARCH = True
SEARCH_BROWSER_PROFILE_DIR = ".tradewheel_search_profile"
SEARCH_BROWSER_HEADLESS = False
SEARCH_BROWSER_TIMEOUT_SECONDS = 45
SEARCH_CLOUDFLARE_WAIT_SECONDS = env_int("TRADEWHEEL_CF_WAIT_SECONDS", 90)
SEARCH_CLOUDFLARE_POLL_SECONDS = env_int("TRADEWHEEL_CF_POLL_SECONDS", 3)
SEARCH_INVALID_WAIT_SECONDS = env_int("TRADEWHEEL_INVALID_WAIT_SECONDS", 15)
PLAYWRIGHT_SEARCH_CHANNEL = "chrome"
PLAYWRIGHT_SEARCH_EXTRA_ARGS = ["--disable-blink-features=AutomationControlled"]

# WEBSITE EMAIL ENRICHMENT (UPGRADED - requests-based, same as EC21/MIC)
ENABLE_WEBSITE_EMAIL_ENRICHMENT = True
MAX_WEBSITE_EMAIL_LOOKUPS = 0
WEBSITE_EMAIL_WORKERS = 10
WEBSITE_TIMEOUT = 15
AUTOSAVE_EVERY_NEW_EMAILS = 1
ENABLE_AI_FILTERING = False
AI_VERIFIED_CSV = "tradewheel_ai_verified.csv"
AI_REJECTED_CSV = "tradewheel_ai_rejected.csv"
AI_CHECKPOINT_CSV = "tradewheel_ai_checkpoint.csv"
AI_BATCH_SIZE = int(os.getenv("AI_BATCH_SIZE", "20"))
AI_CONCURRENT = int(os.getenv("AI_CONCURRENT", "3"))

CONTACT_PATHS = [
    "", "/contact", "/contact-us", "/contactus", "/contact.html",
    "/contact-us.html", "/contactus.html", "/about", "/about-us",
    "/about.html", "/about-us.html", "/contact/", "/contact-us/",
    "/contactus/", "/about/", "/about-us/", "/contactinfo",
    "/contact-info", "/contact_info", "/get-in-touch", "/reach-us",
    "/en/contact", "/en/contact-us",
]

JUNK_EMAIL_PHRASES = [
    "cloudflare", "404", "notfound", "blocked", "error",
    "ordercreditreport", "copyright", "@anytime", "@theforefront",
    "@homeandabroad", "@thistime", "@www", "pleasefeel",
]

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Tradewheel /search/product?country= numeric IDs
COUNTRY_IDS = {
    "China": 43, 
    "South Korea": 111, 
    "Taiwan": 210, 
    "Japan": 103,
    "Vietnam": 220, 
    "Thailand": 201, 
    "Singapore": 183,
    "Malaysia": 145, 
    "Hong Kong": 88,
    "Ukraine": 212,
    "Poland": 166,
    "Czech Republic": 50,
    "Hungary": 92,
    "Romania": 175,
    "Bulgaria": 20,
    "Belarus": 31,
    "Serbia": 244,
    "Croatia": 90,
    "Slovakia": 187,
    "Slovenia": 185,
    "Lithuania": 122,
    "Latvia": 124,
    "Turkey": 207,
}

KEYWORDS = env_list("SCRAPER_KEYWORDS", KEYWORDS)
COUNTRIES = env_list("SCRAPER_COUNTRIES", COUNTRIES)
TARGET_SUPPLIERS = env_int("SCRAPER_TARGET_SUPPLIERS", TARGET_SUPPLIERS)
MAX_LOGGED_IN_ENRICHMENTS = env_int("TRADEWHEEL_MAX_LOGGED_IN_ENRICHMENTS", MAX_LOGGED_IN_ENRICHMENTS)
MAX_WEBSITE_EMAIL_LOOKUPS = env_int("TRADEWHEEL_MAX_EMAIL_LOOKUPS", MAX_WEBSITE_EMAIL_LOOKUPS)

PROXY_POOL = create_proxy_pool("tradewheel")

_TRADEWHEEL_SCRAPLING_CONFIGURED = False
_COUNTRY_FILTER_CACHE: dict[str, dict[str, int]] = {}


def _env_bool(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _configure_scrapling_once() -> None:
    global _TRADEWHEEL_SCRAPLING_CONFIGURED
    if _TRADEWHEEL_SCRAPLING_CONFIGURED:
        return
    cfg: dict[str, object] = {}
    env_map = {
        "adaptive": "TRADEWHEEL_SCRAPLING_ADAPTIVE",
        "huge_tree": "TRADEWHEEL_SCRAPLING_HUGE_TREE",
        "keep_comments": "TRADEWHEEL_SCRAPLING_KEEP_COMMENTS",
        "keep_cdata": "TRADEWHEEL_SCRAPLING_KEEP_CDATA",
        "adaptive_domain": "TRADEWHEEL_SCRAPLING_ADAPTIVE_DOMAIN",
        "storage": "TRADEWHEEL_SCRAPLING_STORAGE",
    }
    for key, env_name in env_map.items():
        raw = os.getenv(env_name)
        if raw is None or raw.strip() == "":
            continue
        value = raw.strip()
        if key in {"adaptive", "huge_tree", "keep_comments", "keep_cdata"}:
            cfg[key] = value.lower() in {"1", "true", "yes", "on"}
        else:
            cfg[key] = value
    if cfg:
        try:
            StealthyFetcher.configure(**cfg)
            print(f"[SCRAPLING][TRADEWHEEL] configured parser options: {tuple(cfg.keys())}")
        except Exception as exc:
            print(f"[SCRAPLING][TRADEWHEEL][WARN] StealthyFetcher.configure failed: {exc}")
    else:
        print("[SCRAPLING][TRADEWHEEL] using default parser configuration")
    _TRADEWHEEL_SCRAPLING_CONFIGURED = True


_configure_scrapling_once()

_CF_INTERSTITIAL_RE = re.compile(
    r"(?:"
    r"checking\s+your\s+browser"
    r"|verify\s+you\s+are\s+human"
    r"|cf-browser-verification"
    r"|cdn-cgi/challenge"
    r"|__cf_chl_"
    r"|challenge-platform"
    r"|/cdn-cgi/l/chk/"
    r")",
    re.I,
)


def _is_cf_challenge_html(html: str) -> bool:
    if not html:
        return False
    if _CF_INTERSTITIAL_RE.search(html):
        return True
    lo = html.lower()
    if "<title>" in lo and "just a moment" in lo and "cloudflare" in lo:
        return True
    return False


def _looks_like_browser_network_error(html: str) -> bool:
    if not html:
        return False
    lo = html.lower()
    return any(
        token in lo
        for token in (
            "err_connection_timed_out",
            "err_timed_out",
            "err_connection_reset",
            "err_tunnel_connection_failed",
            "took too long to respond",
            "can't reach this page",
            "this site can't be reached",
        )
    )


def _tw_has_supplier_listing_signals(html: str) -> bool:
    if not html:
        return False
    lo = html.lower()
    if "/co/" in lo or "company_info.html" in lo or "contact supplier" in lo:
        return True
    if "tradewheel" in lo and re.search(
        r"product\(s\)|suppliers?\s+list|manufacturers?\s+and\s+suppliers|search\s+results\s+for", lo
    ):
        return True
    return False


def _tw_listing_validator(html: str) -> bool:
    if not html or len(html) < 1500:
        return False
    lo = html.lower()
    if "tradewheel" not in lo:
        return False
    if _tw_has_supplier_listing_signals(html):
        return True
    if _is_cf_challenge_html(html):
        return False
    return True


def _generic_http_validator(html: str) -> bool:
    if _is_cf_challenge_html(html):
        return False
    return len(html) > 400


def _validator_for_url(url: str):
    try:
        netloc = (urlparse(url).netloc or "").lower()
    except Exception:
        netloc = ""
    if "tradewheel.com" in netloc:
        return _tw_listing_validator
    return _generic_http_validator


def _proxy_kw_for_stealthy() -> Optional[object]:
    if not PROXY_POOL:
        return None
    ep = PROXY_POOL.get()
    if ep is None:
        return None
    if ep.username:
        return {"server": ep.server, "username": ep.username, "password": ep.password or ""}
    return ep.server


def _stealthy_fetch_with_cloudflare(url: str) -> tuple[int, str]:
    _configure_scrapling_once()
    timeout_ms = int(os.getenv("TRADEWHEEL_STEALTHY_TIMEOUT_MS", "90000") or "90000")
    timeout_ms = max(15000, timeout_ms)
    kwargs: dict = {
        "solve_cloudflare": True,
        "headless": _env_bool("TRADEWHEEL_STEALTHY_HEADLESS", True),
        "timeout": timeout_ms,
        "load_dom": True,
        "google_search": False,
        "extra_headers": DEFAULT_HEADERS,
    }
    px = _proxy_kw_for_stealthy()
    if px is not None:
        kwargs["proxy"] = px
        kwargs["block_webrtc"] = True
    try:
        response = StealthyFetcher.fetch(url, **kwargs)
        status = int(getattr(response, "status", 200) or 200)
        body = getattr(response, "body", None)
        if isinstance(body, (bytes, bytearray)):
            text = body.decode("utf-8", errors="ignore")
        elif isinstance(body, str):
            text = body
        else:
            text = getattr(response, "text", None) or ""
        return status, text or ""
    except Exception as exc:
        print(f"[STEALTHY][TRADEWHEEL][WARN] Cloudflare-capable fetch failed: {exc}")
        return 0, ""


@dataclass
class SupplierRecord:
    company_name: str
    website_url: str = ""
    country: str = ""
    email: str = ""
    source_directory: str = SOURCE_DIRECTORY
    profile_url: str = ""
    company_description: str = ""
    # AI filtering is disabled; keep output CSVs free of AI result columns.
    # is_target_supplier: bool = False
    # confidence: float = 0.0
    # ai_reason: str = ""
    # ai_target_keywords: str = ""


def random_delay():
    time.sleep(random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))


def random_auth_profile_delay():
    low = max(0, AUTH_PROFILE_MIN_DELAY_SECONDS)
    high = max(low, AUTH_PROFILE_MAX_DELAY_SECONDS)
    if high <= 0:
        return
    delay = random.uniform(low, high)
    print(f"[AUTH][WAIT] Waiting {delay:.1f}s before next profile.")
    time.sleep(delay)


def random_auth_cooldown():
    low = max(0, AUTH_COOLDOWN_MIN_SECONDS)
    high = max(low, AUTH_COOLDOWN_MAX_SECONDS)
    if high <= 0:
        return
    delay = random.uniform(low, high)
    print(f"[AUTH][COOLDOWN] Cooling down for {delay / 60:.1f} minutes.")
    time.sleep(delay)


def next_auth_cooldown_after() -> int:
    low = max(0, AUTH_COOLDOWN_EVERY_MIN)
    high = max(low, AUTH_COOLDOWN_EVERY_MAX)
    if high <= 0:
        return 0
    return random.randint(low, high)


def random_auth_show_jitter_ms() -> int:
    low = max(0, AUTH_SHOW_MIN_JITTER_MS)
    high = max(low, AUTH_SHOW_MAX_JITTER_MS)
    return random.randint(low, high)


def random_auth_subpage_delay():
    low = max(0, AUTH_SUBPAGE_MIN_DELAY_SECONDS)
    high = max(low, AUTH_SUBPAGE_MAX_DELAY_SECONDS)
    if high <= 0:
        return
    delay = random.uniform(low, high)
    print(f"[AUTH][WAIT] Waiting {delay:.1f}s before profile subpage.")
    time.sleep(delay)


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def load_phase2_resume_index(path: str = PHASE2_RESUME_STATE_FILE) -> int:
    try:
        value = Path(path).read_text(encoding="utf-8").strip()
        return max(0, int(value or "0"))
    except Exception:
        return 0


def save_phase2_resume_index(index: int, path: str = PHASE2_RESUME_STATE_FILE) -> None:
    Path(path).write_text(str(max(0, int(index))), encoding="utf-8")


def reset_phase2_resume_index(path: str = PHASE2_RESUME_STATE_FILE) -> None:
    save_phase2_resume_index(0, path)


def clean_email(email: str) -> Optional[str]:
    if not email:
        return None
    match = re.match(r'([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})([A-Z].*)?', email)
    if match:
        return match.group(1)
    return email


def extract_email_from_text_flexible(text: str) -> Optional[str]:
    if not text:
        return None
    text = unescape(text)
    mailto_match = re.search(r"mailto:([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})", text, re.I)
    if mailto_match:
        return mailto_match.group(1)
    text = re.sub(r"<[^>]+>", " ", text).lower()
    for pat, rep in [(r"\s*\(at\)\s*", "@"), (r"\s*\[at\]\s*", "@"),
                     (r"\s+at\s+", "@"), (r"\s*\(dot\)\s*", "."),
                     (r"\s*\[dot\]\s*", "."), (r"\s+dot\s+", ".")]:
        text = re.sub(pat, rep, text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", "", text)
    match = re.search(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", text)
    return match.group(0) if match else None


def is_useful_email(email: str) -> bool:
    if not email or len(email) > 50 or len(email) < 6:
        return False
    if ' ' in email or email.count('@') != 1:
        return False
    if not re.search(r'\.(com|net|org|cn|kr|jp|tw|vn|th|sg|my|hk|co\.\w+)$', email):
        return False
    blocked = ["tradewheel.com", "alibaba.com", "made-in-china.com"]
    if any(b in email.lower() for b in blocked):
        return False
    if any(j in email.lower() for j in JUNK_EMAIL_PHRASES):
        return False
    return True


def is_plausible_website(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.netloc or "").lower()
    if not host or "tradewheel.com" in host:
        return False
    blocked = (
        "facebook.com",
        "instagram.com",
        "linkedin.com",
        "youtube.com",
        "youtu.be",
        "wa.me",
        "whatsapp.com",
        "play.google.com",
        "apps.apple.com",
        "itunes.apple.com",
        "appgallery.huawei.com",
        "microsoft.com/store",
        "twitter.com",
        "x.com",
        "pinterest.com",
        "tiktok.com",
        "telegram.me",
        "t.me",
        "wechat.com",
    )
    return not any(t in host for t in blocked)


def normalize_website_url(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    if value.startswith(("http://", "https://")):
        return value
    if value.startswith("www."):
        return f"https://{value}"
    if "." in value and " " not in value:
        return f"https://{value}"
    return ""


def strip_tags(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", text))).strip()


def safe_join_url(url: str) -> str:
    if not url:
        return ""
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/"):
        return f"{BASE_DOMAIN}{url}"
    return f"{BASE_DOMAIN}/{url}"


def build_profile_subpage_urls(profile_url: str) -> list[str]:
    base = (profile_url or "").split("#", 1)[0].split("?", 1)[0].rstrip("/")
    if not base or "/co/" not in base.lower():
        return []
    return [f"{base}/about-us/", f"{base}/contact-us/"]


def build_search_url(keyword: str, country: str = "", page: int = 1, country_id: Optional[int] = None) -> str:
    params: dict[str, str] = {"keyword": (keyword or "").strip()}
    cid = country_id if country_id is not None else COUNTRY_IDS.get(country)
    if cid is not None:
        params["country"] = str(cid)
    if page > 1:
        params["page"] = str(page)
    return f"{BASE_DOMAIN}/search/product?{urlencode(params)}"


def country_filter_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def extract_country_filter_ids(html: str) -> dict[str, int]:
    filters: dict[str, int] = {}
    for href, label in extract_anchor_hrefs_and_text(html):
        href = unescape(href)
        try:
            query = parse_qs(urlparse(href).query)
        except Exception:
            continue
        raw_country = (query.get("country") or [""])[0]
        if not raw_country or not str(raw_country).isdigit():
            continue
        key = country_filter_key(label)
        if key:
            filters[key] = int(raw_country)
    return filters


def fetch_html(url: str) -> tuple[int, str]:
    _configure_scrapling_once()
    validator = _validator_for_url(url)

    if PROXY_POOL is None:
        status, html = _stealthy_fetch_with_cloudflare(url)
        if html and validator(html):
            return status or 200, html
        if html and _looks_like_cloudflare(url, html):
            print("[FETCH][TRADEWHEEL][WARN] StealthyFetcher returned a Cloudflare challenge.")
        elif html:
            print("[FETCH][TRADEWHEEL][WARN] StealthyFetcher returned invalid HTML.")
        return 0, ""

    try:
        response = fetch_with_proxy_rotation(
            fetcher=StealthyFetcher,
            url=url,
            headers=None,
            pool=PROXY_POOL,
            validator=validator,
        )
        status = getattr(response, "status", None) or getattr(response, "status_code", 200)
        body = getattr(response, "body", None)
        if isinstance(body, (bytes, bytearray)):
            text = body.decode("utf-8", errors="ignore")
        elif isinstance(body, str) and body:
            text = body
        elif hasattr(response, "text") and response.text:
            text = response.text
        else:
            text = str(response)
        st = int(status)
        if _env_bool("TRADEWHEEL_STEALTHY_CF_FALLBACK", True) and (
            not text
            or _is_cf_challenge_html(text)
            or not validator(text)
        ):
            sf_status, sf_html = _stealthy_fetch_with_cloudflare(url)
            if sf_html and validator(sf_html):
                return sf_status, sf_html
            if sf_html and _looks_like_cloudflare(url, sf_html):
                print("[FETCH][TRADEWHEEL][WARN] StealthyFetcher still returned a Cloudflare challenge.")
            elif sf_html:
                print("[FETCH][TRADEWHEEL][WARN] StealthyFetcher returned invalid HTML.")
            return 0, ""
        return st, text
    except ProxyExhaustedError as exc:
        print(f"[PROXY][WARN] {exc}")
        if _env_bool("TRADEWHEEL_STEALTHY_CF_FALLBACK", True):
            sf_status, sf_html = _stealthy_fetch_with_cloudflare(url)
            if sf_html and validator(sf_html):
                return sf_status or 200, sf_html
        return 0, ""
    except Exception as exc:
        if _env_bool("TRADEWHEEL_STEALTHY_CF_FALLBACK", True):
            print(f"[FETCH][TRADEWHEEL][WARN] {exc!r}; retrying with StealthyFetcher (solve_cloudflare)")
            sf_status, sf_html = _stealthy_fetch_with_cloudflare(url)
            if sf_html and validator(sf_html):
                return sf_status or 200, sf_html
        return 0, ""


def _looks_like_cloudflare(url: str, html: str) -> bool:
    u = (url or "").lower()
    if "cdn-cgi/challenge" in u or "challenges.cloudflare.com" in u:
        return True
    return _is_cf_challenge_html(html)


def _cloudflare_search_fallback(url: str, reason: str = "") -> tuple[int, str]:
    if not _env_bool("TRADEWHEEL_STEALTHY_CF_FALLBACK", True):
        return 0, ""
    label = f" ({reason})" if reason else ""
    print(f"[SEARCH][TRADEWHEEL] Cloudflare/blocked page detected{label}; trying StealthyFetcher.")
    status, html = _stealthy_fetch_with_cloudflare(url)
    if html and _tw_listing_validator(html):
        return status or 200, html
    if html and _looks_like_cloudflare(url, html):
        print("[SEARCH][TRADEWHEEL][WARN] StealthyFetcher still returned a Cloudflare challenge.")
    elif html:
        print("[SEARCH][TRADEWHEEL][WARN] StealthyFetcher returned non-listing HTML.")
    return 0, ""


def _dismiss_tradewheel_signup_modal(page) -> None:
    if page is None:
        return
    selectors = (
        "#signup_modal button.close",
        "#signup_modal .modal-header button.close",
        "section.signup-popup-s button.close[data-dismiss='modal']",
    )
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() == 0:
                continue
            if not loc.is_visible(timeout=2500):
                continue
            loc.click(timeout=4000)
            page.wait_for_timeout(400)
            return
        except Exception:
            continue


class SearchPageBrowser:
    def __init__(self, timeout_seconds: int = SEARCH_BROWSER_TIMEOUT_SECONDS):
        self.timeout_ms = max(5, int(timeout_seconds)) * 1000
        self.playwright = None
        self.context = None
        self.page = None
        self._proxy_endpoint: Optional[ProxyEndpoint] = None
        self.enabled = sync_playwright is not None and USE_BROWSER_FOR_SEARCH

    def _relaunch_persistent(self, ep: Optional[ProxyEndpoint]):
        if self.context:
            try: self.context.close()
            except: pass
        self.context = None
        self.page = None
        launch_kwargs = dict(
            user_data_dir=str(Path(SEARCH_BROWSER_PROFILE_DIR).resolve()),
            headless=SEARCH_BROWSER_HEADLESS,
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="Asia/Shanghai",
            args=PLAYWRIGHT_SEARCH_EXTRA_ARGS,
        )
        if PLAYWRIGHT_SEARCH_CHANNEL:
            launch_kwargs["channel"] = PLAYWRIGHT_SEARCH_CHANNEL
        if PROXY_POOL:
            cfg = PROXY_POOL.playwright_config(ep) if ep is not None else PROXY_POOL.playwright_config()
            if cfg:
                launch_kwargs["proxy"] = cfg
        try:
            self.context = self.playwright.chromium.launch_persistent_context(**launch_kwargs)
        except Exception as exc:
            if launch_kwargs.pop("channel", None):
                print(f"[SEARCH][WARN] Could not launch channel '{PLAYWRIGHT_SEARCH_CHANNEL}': {exc}")
                self.context = self.playwright.chromium.launch_persistent_context(**launch_kwargs)
            else:
                raise
        self.page = self.context.new_page()
        self._proxy_endpoint = ep
        return self.page

    def __enter__(self):
        if not self.enabled:
            return self
        self.playwright = sync_playwright().start()
        self._proxy_endpoint = PROXY_POOL.get() if PROXY_POOL else None
        self._relaunch_persistent(self._proxy_endpoint)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.context: self.context.close()
        finally:
            if self.playwright: self.playwright.stop()

    def _wait_for_listing_html(self, url: str) -> tuple[int, str]:
        deadline = time.monotonic() + max(5, SEARCH_CLOUDFLARE_WAIT_SECONDS)
        invalid_deadline = time.monotonic() + max(3, SEARCH_INVALID_WAIT_SECONDS)
        announced_cf_wait = False
        last_html = ""

        while time.monotonic() < deadline:
            try:
                self.page.wait_for_timeout(max(1, SEARCH_CLOUDFLARE_POLL_SECONDS) * 1000)
                _dismiss_tradewheel_signup_modal(self.page)
                html = self.page.content()
            except Exception:
                raise

            last_html = html or ""
            if _tw_listing_validator(last_html):
                return 200, last_html

            if _looks_like_cloudflare(url, last_html):
                if not announced_cf_wait:
                    print(
                        f"[SEARCH][TRADEWHEEL] Cloudflare challenge detected; "
                        f"waiting up to {SEARCH_CLOUDFLARE_WAIT_SECONDS}s for the browser session to clear it."
                    )
                    announced_cf_wait = True
                continue

            if last_html and len(last_html) >= 1500 and time.monotonic() >= invalid_deadline:
                return 0, ""

        if last_html and _looks_like_cloudflare(url, last_html):
            print("[SEARCH][TRADEWHEEL][WARN] Cloudflare challenge did not clear in time.")
        return 0, ""

    def fetch(self, url: str) -> tuple[int, str]:
        if not self.enabled or self.page is None:
            return 0, ""
        for attempt in range(2):
            try:
                if PROXY_POOL:
                    def _rel(ep): return self._relaunch_persistent(ep)
                    self.page, self._proxy_endpoint = goto_with_rotation(
                        self.page, url, PROXY_POOL, _rel,
                        current_endpoint=self._proxy_endpoint,
                        timeout_ms=self.timeout_ms,
                        wait_until="domcontentloaded",
                        validate=_tw_listing_validator,
                    )
                else:
                    self.page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                return self._wait_for_listing_html(url)
            except Exception as exc:
                if attempt == 0:
                    print(f"[SEARCH][TRADEWHEEL][WARN] Browser fetch failed ({type(exc).__name__}); relaunching session.")
                    try:
                        self._relaunch_persistent(self._proxy_endpoint)
                    except Exception as relaunch_exc:
                        print(f"[SEARCH][TRADEWHEEL][WARN] Browser relaunch failed: {relaunch_exc}")
                        return 0, ""
                    continue
                print(f"[SEARCH][TRADEWHEEL][WARN] Browser fetch failed after relaunch: {exc}")
                return 0, ""
        return 0, ""


def extract_anchor_hrefs_and_text(html: str) -> list[tuple[str, str]]:
    anchors = []
    for href, inner in re.findall(
        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        html, flags=re.IGNORECASE | re.DOTALL,
    ):
        anchors.append((safe_join_url(href.strip()), strip_tags(inner)))
    return anchors


def extract_external_website_from_profile(profile_html: str) -> str:
    row_matches = re.findall(
        r"<tr[^>]*>\s*<td[^>]*>\s*Website:\s*</td>\s*<td[^>]*>(.*?)</td>\s*</tr>",
        profile_html, flags=re.IGNORECASE | re.DOTALL,
    )
    for value_cell in row_matches:
        href_match = re.search(r'<a[^>]+href=["\']([^"\']+)["\']', value_cell, re.I)
        href = safe_join_url(href_match.group(1).strip()) if href_match else ""
        if is_plausible_website(href):
            return href
        website_candidate = normalize_website_url(strip_tags(value_cell))
        if is_plausible_website(website_candidate):
            return website_candidate
    return ""


def extract_email_from_profile_table(profile_html: str) -> str:
    row_matches = re.findall(
        r"<tr[^>]*>\s*<td[^>]*>\s*(?:Email|E-mail)\s*:\s*</td>\s*<td[^>]*>(.*?)</td>\s*</tr>",
        profile_html, flags=re.IGNORECASE | re.DOTALL,
    )
    for value_cell in row_matches:
        email = extract_email_from_text_flexible(strip_tags(value_cell))
        email = clean_email(email)
        if email and is_useful_email(email):
            return email
    return ""


def extract_main_products_from_profile(profile_html: str) -> str:
    row_matches = re.findall(
        r"<tr[^>]*>\s*<td[^>]*>\s*Main\s+Products\s*:?\s*</td>\s*<td[^>]*>(.*?)</td>\s*</tr>",
        profile_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    for value_cell in row_matches:
        products = strip_tags(value_cell)
        if products:
            return f"Main Products: {products}"
    return ""


def parse_company_anchors(html: str) -> list[dict[str, str]]:
    anchors = []
    seen = set()
    for href, name in extract_anchor_hrefs_and_text(html):
        if "/co/" not in href.lower():
            continue
        if not href or not name or href in seen:
            continue
        seen.add(href)
        anchors.append({"href": href, "name": name})
    return anchors


def extract_company_record(anchor: dict[str, str], keyword: str) -> SupplierRecord:
    return SupplierRecord(
        company_name=anchor.get("name", "").strip() or "Unknown Supplier",
        profile_url=safe_join_url(anchor.get("href", "")),
    )


def resolve_country_id_for_keyword(keyword: str, country: str, search_browser: "SearchPageBrowser") -> Optional[int]:
    cid = COUNTRY_IDS.get(country)
    if cid is not None:
        return cid

    keyword_key = (keyword or "").strip().lower()
    if keyword_key not in _COUNTRY_FILTER_CACHE:
        base_url = build_search_url(keyword=keyword, page=1)
        try:
            if search_browser.enabled:
                status, html = search_browser.fetch(base_url)
            else:
                status, html = fetch_html(base_url)
        except Exception:
            status, html = 0, ""
        _COUNTRY_FILTER_CACHE[keyword_key] = extract_country_filter_ids(html) if status and html else {}

    filters = _COUNTRY_FILTER_CACHE.get(keyword_key, {})
    cid = filters.get(country_filter_key(country))
    if cid is not None:
        COUNTRY_IDS[country] = cid
        print(f"  [{keyword}] [{country}] Found TradeWheel country filter id: {cid}")
        return cid

    print(f"  [{keyword}] [{country}] No TradeWheel country filter available; skipping.")
    return None


class LoggedInContactEnricher:
    """Playwright-powered enrichment for logged-in contact reveal."""

    def __init__(self, max_attempts: int = MAX_LOGGED_IN_ENRICHMENTS,
                 pause_for_manual_login: bool = PAUSE_FOR_MANUAL_LOGIN):
        self.max_attempts = max_attempts
        self.pause_for_manual_login = pause_for_manual_login
        self.attempts = 0
        self.playwright = None
        self.context = None
        self.page = None
        self._proxy_endpoint: Optional[ProxyEndpoint] = None
        self.last_access_failed = False
        self.enabled = sync_playwright is not None and ENABLE_LOGGED_IN_ENRICHMENT
        self.auth_email = os.getenv(TRADEWHEEL_EMAIL_ENV, "").strip()
        self.auth_password = os.getenv(TRADEWHEEL_PASSWORD_ENV, "")

    def _relaunch_auth_persistent(self, ep: Optional[ProxyEndpoint]):
        if self.context:
            try: self.context.close()
            except: pass
        self.context = None
        self.page = None
        launch_kwargs_base = dict(
            user_data_dir=str(Path(AUTH_BROWSER_PROFILE_DIR).resolve()),
            headless=AUTH_BROWSER_HEADLESS,
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="Asia/Shanghai",
            args=PLAYWRIGHT_AUTH_EXTRA_ARGS,
        )
        if PLAYWRIGHT_AUTH_CHANNEL:
            launch_kwargs_base["channel"] = PLAYWRIGHT_AUTH_CHANNEL
        if AUTH_USE_PROXY and PROXY_POOL:
            cfg = PROXY_POOL.playwright_config(ep) if ep is not None else PROXY_POOL.playwright_config()
            if cfg:
                launch_kwargs_base["proxy"] = cfg

        attempts = []
        attempts.append(dict(launch_kwargs_base))
        if "channel" in launch_kwargs_base:
            without_channel = dict(launch_kwargs_base)
            without_channel.pop("channel", None)
            attempts.append(without_channel)
        if "proxy" in launch_kwargs_base:
            without_proxy = dict(launch_kwargs_base)
            without_proxy.pop("proxy", None)
            attempts.append(without_proxy)

        last_exc = None
        for attempt_no, launch_kwargs in enumerate(attempts, 1):
            try:
                self.context = self.playwright.chromium.launch_persistent_context(**launch_kwargs)
                existing_pages = []
                try:
                    existing_pages = list(self.context.pages)
                except Exception:
                    existing_pages = []
                self.page = existing_pages[0] if existing_pages else self.context.new_page()
                self._proxy_endpoint = ep if "proxy" in launch_kwargs else None
                return self.page
            except Exception as exc:
                last_exc = exc
                label = []
                if "channel" not in launch_kwargs and PLAYWRIGHT_AUTH_CHANNEL:
                    label.append("no channel")
                if "proxy" not in launch_kwargs and "proxy" in launch_kwargs_base:
                    label.append("no proxy")
                detail = f" ({', '.join(label)})" if label else ""
                print(f"[AUTH][WARN] Browser launch attempt {attempt_no}/{len(attempts)}{detail} failed: {exc}")
                try:
                    if self.context:
                        self.context.close()
                except Exception:
                    pass
                self.context = None
                self.page = None

        raise last_exc

    def _auth_goto(self, url: str, timeout_ms: int = 120000) -> None:
        if self.page is None:
            return
        if not AUTH_USE_PROXY or not PROXY_POOL:
            self.page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            return
        def _rel(e): return self._relaunch_auth_persistent(e)
        self.page, self._proxy_endpoint = goto_with_rotation(
            self.page, url, PROXY_POOL, _rel,
            current_endpoint=self._proxy_endpoint,
            timeout_ms=timeout_ms,
            wait_until="domcontentloaded",
            validate=lambda h: "tradewheel" in h.lower() and len(h) > 800,
        )

    def _wait_for_cloudflare_clear(self, url: str) -> bool:
        if self.page is None:
            return False

        for reload_attempt in range(AUTH_CLOUDFLARE_RELOAD_ATTEMPTS + 1):
            deadline = time.monotonic() + max(10, AUTH_CLOUDFLARE_WAIT_SECONDS)
            announced = False
            last_html = ""

            while time.monotonic() < deadline:
                try:
                    self.page.wait_for_timeout(max(1, AUTH_CLOUDFLARE_POLL_SECONDS) * 1000)
                    html = self.page.content() or ""
                except Exception:
                    return False

                last_html = html
                current_url = ""
                try:
                    current_url = self.page.url or url
                except Exception:
                    current_url = url

                if _looks_like_browser_network_error(html):
                    print("[AUTH][TRADEWHEEL][WARN] Browser hit a network timeout/error page. Saving progress and stopping Phase 2 is safer.")
                    self.last_access_failed = True
                    return False

                if _looks_like_cloudflare(current_url, html):
                    if not announced:
                        extra = (
                            f" reload attempt {reload_attempt}/{AUTH_CLOUDFLARE_RELOAD_ATTEMPTS};"
                            if reload_attempt
                            else ""
                        )
                        print(
                            f"[AUTH][TRADEWHEEL] Cloudflare challenge detected;{extra} "
                            f"waiting up to {AUTH_CLOUDFLARE_WAIT_SECONDS}s for the browser session to clear it."
                        )
                        announced = True
                    if AUTH_STOP_ON_CLOUDFLARE:
                        print("[AUTH][TRADEWHEEL][STOP] Cloudflare detected. Saving progress and stopping Phase 2.")
                        self.last_access_failed = True
                        return False
                    continue

                if html and "tradewheel" in html.lower() and len(html) > 800:
                    return True

            if last_html and _looks_like_cloudflare(url, last_html):
                print("[AUTH][TRADEWHEEL][WARN] Cloudflare challenge did not clear in time.")
                if reload_attempt < AUTH_CLOUDFLARE_RELOAD_ATTEMPTS:
                    try:
                        print("[AUTH][TRADEWHEEL] Reloading profile page and retrying Cloudflare wait.")
                        self.page.goto(url, wait_until="domcontentloaded", timeout=90000)
                        continue
                    except Exception:
                        return False
            break

        self.last_access_failed = True
        return False

    def __enter__(self):
        if not self.enabled:
            return self
        self.playwright = sync_playwright().start()
        self._proxy_endpoint = PROXY_POOL.get() if AUTH_USE_PROXY and PROXY_POOL else None
        self._relaunch_auth_persistent(self._proxy_endpoint)
        if self.pause_for_manual_login:
            auto_login_ok = self._login_with_env()
            if not auto_login_ok:
                print("\n[AUTH] Browser opened for Tradewheel login.")
                try:
                    self._auth_goto("https://www.tradewheel.com/login/", 120000)
                except: pass
                self._wait_for_manual_login(timeout_seconds=240)
        return self

    def _first_visible(self, selectors: tuple[str, ...]):
        for selector in selectors:
            try:
                locator = self.page.locator(selector).first
                if locator.count() > 0 and locator.is_visible(timeout=2000):
                    return locator
            except: continue
        return None

    def _is_logged_in(self) -> bool:
        if self.page is None: return False
        try: url = (self.page.url or "").lower()
        except: url = ""
        on_auth = ("/login" in url) or ("/signin" in url) or ("/sign-in" in url)
        if url and not on_auth: return True
        try: pw = self.page.locator("input[type='password']").first.is_visible(timeout=1500)
        except: pw = False
        return not pw

    def _wait_for_manual_login(self, timeout_seconds: int = 240) -> bool:
        if self.page is None: return False
        deadline = time.monotonic() + max(5, int(timeout_seconds))
        while time.monotonic() < deadline:
            if self._is_logged_in(): return True
            try: self.page.wait_for_timeout(2000)
            except: time.sleep(2)
        return False

    def _login_with_env(self) -> bool:
        if self.page is None: return False
        try:
            self._auth_goto("https://www.tradewheel.com/login/", 120000)
            self.page.wait_for_timeout(1500)
        except: return False
        if self._is_logged_in():
            print("[AUTH] Existing Tradewheel session detected.")
            return True
        if not self.auth_email or not self.auth_password: return False
        try:
            email_input = self._first_visible((
                "input[type='email']", "input[name='email']", "input[name='username']",
            ))
            password_input = self._first_visible((
                "input[type='password']", "input[name='password']",
            ))
            if not email_input or not password_input: return False
            email_input.fill(self.auth_email, timeout=10000)
            password_input.fill(self.auth_password, timeout=10000)
            submit = self._first_visible((
                "button[type='submit']", "input[type='submit']",
                "button:has-text('Login')", "button:has-text('Sign In')",
            ))
            if submit: submit.click(timeout=10000)
            else: password_input.press("Enter")
            try: self.page.wait_for_load_state("networkidle", timeout=30000)
            except: self.page.wait_for_timeout(5000)
            return self._is_logged_in()
        except: return False

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.context: self.context.close()
        finally:
            if self.playwright: self.playwright.stop()

    def can_run(self) -> bool:
        return (
            self.enabled
            and self.page is not None
            and (self.max_attempts <= 0 or self.attempts < self.max_attempts)
        )

    def _current_profile_html(self, profile_url: str) -> str:
        html = ""
        for _ in range(3):
            try:
                self.page.wait_for_load_state("domcontentloaded", timeout=7000)
                html = self.page.content()
                if _looks_like_browser_network_error(html):
                    print("[AUTH][TRADEWHEEL][WARN] Browser hit a network timeout/error page while reading profile HTML.")
                    self.last_access_failed = True
                    return ""
                if _looks_like_cloudflare(self.page.url or profile_url, html):
                    if not self._wait_for_cloudflare_clear(profile_url):
                        return ""
                    html = self.page.content()
                if html:
                    break
            except:
                self.page.wait_for_timeout(800)
        return html or ""

    def _visit_profile_subpage(self, url: str) -> str:
        try:
            random_auth_subpage_delay()
            self._auth_goto(url, 90000)
            if not self._wait_for_cloudflare_clear(url):
                return ""
            _dismiss_tradewheel_signup_modal(self.page)
            return self._current_profile_html(url)
        except:
            return ""

    def enrich(self, profile_url: str) -> tuple[str, str, str]:
        if not self.can_run() or not profile_url:
            return "", "", ""
        self.attempts += 1
        self.last_access_failed = False
        try:
            self._auth_goto(profile_url, 90000)
            if not self._wait_for_cloudflare_clear(profile_url):
                return "", "", ""
            _dismiss_tradewheel_signup_modal(self.page)
            reveal_xpaths = [
                "//tr[td[contains(translate(normalize-space(.), 'WEBSITE', 'website'), 'website')]]//a[contains(., 'Show')]",
            ]
            if AUTH_REVEAL_EMAIL:
                reveal_xpaths.append(
                    "//tr[td[contains(translate(normalize-space(.), 'EMAIL', 'email'), 'email')]]//a[contains(., 'Show')]"
                )
            for xpath in reveal_xpaths:
                try:
                    link = self.page.locator(f"xpath={xpath}").first
                    if link.count() > 0:
                        self.page.wait_for_timeout(random_auth_show_jitter_ms())
                        link.click(timeout=2500)
                        self.page.wait_for_timeout(random_auth_show_jitter_ms())
                except: continue
        except Exception as exc:
            print(f"[AUTH][TRADEWHEEL][WARN] Profile navigation failed: {type(exc).__name__}")
            self.last_access_failed = True
            return "", "", ""
        html = self._current_profile_html(profile_url)
        if not html: return "", "", ""
        website_url = extract_external_website_from_profile(html)
        email = extract_email_from_profile_table(html)
        description = extract_main_products_from_profile(html)

        if AUTH_VISIT_PROFILE_SUBPAGES and (not website_url or not description):
            for subpage_url in build_profile_subpage_urls(profile_url):
                subpage_html = self._visit_profile_subpage(subpage_url)
                if not subpage_html:
                    continue
                if not website_url:
                    website_url = extract_external_website_from_profile(subpage_html)
                if AUTH_REVEAL_EMAIL and not email:
                    email = extract_email_from_profile_table(subpage_html)
                if not description:
                    description = extract_main_products_from_profile(subpage_html)
                if website_url and description:
                    break

        return website_url, email, description


def run_logged_in_enrichment(
    records: list[SupplierRecord],
    max_records: Optional[int] = None,
    checkpoint_path: str = PARTIAL_OUTPUT_CSV,
    save_every_processed: int = 5,
) -> bool:
    if not ENABLE_LOGGED_IN_ENRICHMENT: return True
    with LoggedInContactEnricher() as auth_enricher:
        if not auth_enricher.enabled: return True
        processed = 0
        updated = 0
        consecutive_access_failures = 0
        next_cooldown_at = next_auth_cooldown_after()
        resume_after_index = load_phase2_resume_index()
        if resume_after_index:
            print(f"[AUTH][RESUME] Skipping first {resume_after_index} Phase 2 rows from saved state.")
        for index, record in enumerate(records, 1):
            if max_records and processed >= max_records: break
            if index <= resume_after_index:
                continue
            if record.website_url and not is_plausible_website(record.website_url):
                print(f"[AUTH][CLEAN] Removing invalid website for {record.company_name[:60]}: {record.website_url}")
                record.website_url = ""
            if record.website_url and record.email and record.company_description:
                save_phase2_resume_index(index)
                continue
            if not auth_enricher.can_run(): break
            website_url, email, description = auth_enricher.enrich(record.profile_url)
            changed = False
            if website_url and not record.website_url:
                record.website_url = website_url
                changed = True
            if email and not record.email and is_useful_email(email):
                record.email = email
                changed = True
            if description and not record.company_description:
                record.company_description = description
                changed = True
            processed += 1
            save_phase2_resume_index(index)
            if auth_enricher.last_access_failed:
                consecutive_access_failures += 1
                save_checkpoint(records, checkpoint_path)
                print(
                    f"[AUTH][STOP-GUARD] Access failure {consecutive_access_failures}/"
                    f"{AUTH_MAX_CONSECUTIVE_ACCESS_FAILURES}. Progress saved."
                )
                if consecutive_access_failures >= AUTH_MAX_CONSECUTIVE_ACCESS_FAILURES:
                    print("[AUTH][STOP-GUARD] Repeated access failures detected; stopping Phase 2 for this session.")
                    print("[AUTH][STOP-GUARD] Resume later with: python tradewheel_scraper.py --resume-from-phase2")
                    return False
            else:
                consecutive_access_failures = 0
            if changed:
                updated += 1
                save_checkpoint(records, checkpoint_path)
                print(
                    f"[AUTH][SAVE] {index}/{len(records)} "
                    f"updated={updated} company={record.company_name[:60]}"
                )
            elif save_every_processed > 0 and processed % save_every_processed == 0:
                save_checkpoint(records, checkpoint_path)
                print(f"[AUTH][SAVE] processed={processed} updated={updated}")
            if processed % 25 == 0:
                print(f"[AUTH] Processed {processed} profiles; updated {updated}.")
            should_continue = (
                (not max_records or processed < max_records)
                and auth_enricher.can_run()
            )
            if should_continue and next_cooldown_at and processed >= next_cooldown_at:
                save_checkpoint(records, checkpoint_path)
                random_auth_cooldown()
                next_cooldown_at = processed + next_auth_cooldown_after()
            if should_continue:
                random_auth_profile_delay()
    return True


# ===== PHASE 3: WEBSITE EMAIL ENRICHMENT (REQUESTS-BASED) =====

def fetch_page_requests(url: str) -> Optional[str]:
    """Fetch with requests for email enrichment. Returns HTML or None."""
    try:
        resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=WEBSITE_TIMEOUT, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
        return None
    except Exception:
        return None


def extract_email_from_html(html: str) -> Optional[str]:
    """Extract email from HTML - checks footer first, then full page."""
    if not html:
        return None
    text = unescape(html)
    footer_patterns = [
        r'<footer[^>]*>(.*?)</footer>',
        r'<div[^>]*class="[^"]*footer[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*id="[^"]*footer[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*class="[^"]*contact[^"]*"[^>]*>(.*?)</div>',
    ]
    for pattern in footer_patterns:
        match = re.search(pattern, text, re.I | re.DOTALL)
        if match:
            email = _find_email(match.group(1))
            if email: return email
    return _find_email(text)


def _find_email(text: str) -> Optional[str]:
    text = text.lower()
    mailto = re.search(r"mailto:([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})", text, re.I)
    if mailto:
        email = mailto.group(1)
        if is_useful_email(email): return email
    text = re.sub(r"<[^>]+>", " ", text)
    for pat, rep in [(r"\s*\(at\)\s*", "@"), (r"\s*\[at\]\s*", "@"),
                     (r"\s+at\s+", "@"), (r"\s*\(dot\)\s*", "."),
                     (r"\s*\[dot\]\s*", "."), (r"\s+dot\s+", ".")]:
        text = re.sub(pat, rep, text, flags=re.I)
    text = re.sub(r"\s+", "", text)
    match = re.search(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", text)
    if match and is_useful_email(match.group(0)):
        return match.group(0)
    return None


def lookup_email_requests(website_url: str) -> str:
    """Try multiple contact page URLs until email is found."""
    if not website_url or str(website_url) == 'nan' or website_url == '':
        return ""
    base = website_url.rstrip("/")
    for path in CONTACT_PATHS:
        url = f"{base}{path}" if path else base
        html = fetch_page_requests(url)
        if html:
            email = extract_email_from_html(html)
            if email:
                return email
    return ""


def enrich_emails_from_company_websites(
    records: list[SupplierRecord],
    checkpoint_records: Optional[list[SupplierRecord]] = None,
    checkpoint_path: str = PARTIAL_OUTPUT_CSV,
):
    """Visit company websites to find emails (requests-based, same as EC21/MIC)."""
    if not ENABLE_WEBSITE_EMAIL_ENRICHMENT:
        return
    records_to_save = checkpoint_records if checkpoint_records is not None else records
    
    for record in records:
        if record.website_url and not is_plausible_website(record.website_url):
            record.website_url = ""

    candidates = [r for r in records if r.website_url and not r.email]
    if MAX_WEBSITE_EMAIL_LOOKUPS > 0:
        candidates = candidates[:MAX_WEBSITE_EMAIL_LOOKUPS]
    if not candidates:
        return
    
    print(f"\n[WEBSITE-EMAIL] Looking for emails on {len(candidates)} company websites...")
    print(f"  Strategy: Footer → Homepage → {len(CONTACT_PATHS)} contact paths | Timeout: {WEBSITE_TIMEOUT}s")
    
    found = 0
    skipped = 0
    emails_since_save = 0
    
    with ThreadPoolExecutor(max_workers=WEBSITE_EMAIL_WORKERS) as executor:
        futures = {executor.submit(lookup_email_requests, r.website_url): r for r in candidates}
        total = len(candidates)
        
        for i, future in enumerate(as_completed(futures), 1):
            record = futures[future]
            try:
                email = future.result(timeout=WEBSITE_TIMEOUT + 5)
            except FutureTimeoutError:
                skipped += 1
                continue
            
            if email:
                record.email = email
                found += 1
                emails_since_save += 1
                print(f"  [{found}] {record.company_name[:40]} → {email}")
                
                if emails_since_save >= AUTOSAVE_EVERY_NEW_EMAILS:
                    save_checkpoint(records_to_save, checkpoint_path)
                    print(f"  [SAVE] {found} emails → {checkpoint_path}")
                    emails_since_save = 0
            
            if i % 50 == 0:
                print(f"  Progress: {i}/{total}, found {found}, skipped {skipped}")
    
    save_checkpoint(records_to_save, checkpoint_path)
    print(f"  Total emails found: {found} ({skipped} timed out)")


# ===== SAVE FUNCTIONS =====

def save_checkpoint(records: list[SupplierRecord], path: str):
    if not records:
        return
    for record in records:
        if record.website_url and not is_plausible_website(record.website_url):
            record.website_url = ""
    df = pd.DataFrame([asdict(r) for r in records])
    df["name_norm"] = df["company_name"].str.lower().str.strip()
    df = df.drop_duplicates(subset=["name_norm"]).drop(columns=["name_norm"])
    df.to_csv(path, index=False, encoding="utf-8-sig")


def filter_records_with_company_websites(records: list[SupplierRecord]) -> list[SupplierRecord]:
    kept = [record for record in records if record.website_url and is_plausible_website(record.website_url)]
    removed = len(records) - len(kept)
    print(f"[CLEANUP] Keeping {len(kept)} records with company websites; removed {removed} before email enrichment.")
    return kept


def _csv_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _csv_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return _csv_text(value).lower() in {"1", "true", "yes", "y"}


def _csv_float(value: object) -> float:
    if value is None or pd.isna(value):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def load_records_from_checkpoint(path: str) -> list[SupplierRecord]:
    df = pd.read_csv(path, encoding="utf-8-sig")
    records: list[SupplierRecord] = []
    for row in df.to_dict("records"):
        records.append(
            SupplierRecord(
                company_name=_csv_text(row.get("company_name")) or "Unknown Supplier",
                website_url=_csv_text(row.get("website_url")),
                country=_csv_text(row.get("country")),
                email=_csv_text(row.get("email")),
                source_directory=_csv_text(row.get("source_directory")) or SOURCE_DIRECTORY,
                profile_url=_csv_text(row.get("profile_url")),
                company_description=_csv_text(row.get("company_description")),
            )
        )
    return records


# ===== MAIN =====

def scrape_tradewheel(resume_from_phase2: bool = False, resume_input: str = PARTIAL_OUTPUT_CSV):
    all_records = []
    seen_names = set()
    save_counter = 0

    print("=" * 60)
    print("Tradewheel Cosmetic Packaging Supplier Scraper")
    print(f"Target: {TARGET_SUPPLIERS} suppliers")
    print(f"Email: requests-based, {len(CONTACT_PATHS)} contact paths, {WEBSITE_TIMEOUT}s timeout")
    print("=" * 60)

    if resume_from_phase2:
        all_records = load_records_from_checkpoint(resume_input)
        print(f"[RESUME] Loaded {len(all_records)} records from {resume_input}")
        save_checkpoint(all_records, PARTIAL_OUTPUT_CSV)
    else:
        try:
            with SearchPageBrowser() as search_browser:
                if search_browser.enabled:
                    print(f"[SEARCH] Using persistent browser session for listings.")
                else:
                    print("[SEARCH] Browser session unavailable; falling back to fetcher.")
                
                for keyword in KEYWORDS:
                    if len(all_records) >= TARGET_SUPPLIERS: break
                    for country in COUNTRIES:
                        if len(all_records) >= TARGET_SUPPLIERS: break
                        country_id = resolve_country_id_for_keyword(keyword, country, search_browser)
                        if country_id is None:
                            continue
                        zero_data_pages = 0
                        for page in range(1, MAX_PAGES_PER_QUERY + 1):
                            if len(all_records) >= TARGET_SUPPLIERS: break

                            url = build_search_url(keyword, country, page, country_id=country_id)
                            try:
                                if search_browser.enabled:
                                    status, html = search_browser.fetch(url)
                                else:
                                    status, html = fetch_html(url)
                            except:
                                random_delay()
                                continue

                            if status == 404 or not html:
                                zero_data_pages += 1
                                print(
                                    f"  [{keyword}] [{country}] Page {page}: 0 data "
                                    f"({zero_data_pages}/1 empty)"
                                )
                                if zero_data_pages >= 1:
                                    print(f"  [{keyword}] [{country}] 1 empty pages; moving to next combination.")
                                    break
                                random_delay()
                                continue
                            anchors = parse_company_anchors(html)
                            if not anchors:
                                zero_data_pages += 1
                                print(
                                    f"  [{keyword}] [{country}] Page {page}: 0 data "
                                    f"({zero_data_pages}/1 empty)"
                                )
                                if zero_data_pages >= 1:
                                    print(f"  [{keyword}] [{country}] 1 empty pages; moving to next combination.")
                                    break
                                random_delay()
                                continue

                            page_new = 0
                            for anchor in anchors:
                                norm = re.sub(r"\s+", " ", anchor["name"]).strip().lower()
                                if not norm or norm in seen_names: continue
                                record = extract_company_record(anchor, keyword)
                                record.country = country
                                seen_names.add(norm)
                                all_records.append(record)
                                page_new += 1
                                save_counter += 1
                                if save_counter >= AUTOSAVE_EVERY_NEW_RECORDS:
                                    save_checkpoint(all_records, PARTIAL_OUTPUT_CSV)
                                    print(f"[CHECKPOINT] Saved {len(all_records)} records")
                                    save_counter = 0

                            if page_new:
                                zero_data_pages = 0
                            else:
                                zero_data_pages += 1
                            print(f"  [{keyword}] [{country}] Page {page}: +{page_new} (Total: {len(all_records)})")
                            if zero_data_pages >= 1:
                                print(f"  [{keyword}] [{country}] 1 empty pages; moving to next combination.")
                                break
                            random_delay()

        except KeyboardInterrupt:
            print("\n[INTERRUPTED] Saving progress...")
            save_checkpoint(all_records, PARTIAL_OUTPUT_CSV)
        except Exception as e:
            print(f"\n[ERROR] {e}")
            save_checkpoint(all_records, PARTIAL_OUTPUT_CSV)

    # Phase 2: Logged-in enrichment (Playwright - unchanged)
    phase2_completed = True
    if all_records and ENABLE_LOGGED_IN_ENRICHMENT:
        print("\n[AUTH] Starting logged-in enrichment...")
        try:
            phase2_completed = run_logged_in_enrichment(all_records)
        except Exception as e:
            print(f"[AUTH] Error: {e}")
            phase2_completed = False
        save_checkpoint(all_records, PARTIAL_OUTPUT_CSV)

    if not phase2_completed:
        print("[AUTH] Phase 2 stopped early. Skipping website email enrichment so resume state is preserved.")
        df = pd.DataFrame([asdict(r) for r in all_records])
        if df.empty:
            return df
        df["name_norm"] = df["company_name"].str.lower().str.strip()
        df = df.drop_duplicates(subset=["name_norm"]).drop(columns=["name_norm"])
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        df_clean = df[df['email'].notna() & (df['email'] != '')]
        df_clean.to_csv(CLEANED_CSV, index=False, encoding="utf-8-sig")
        return df

    ai_verified_records = all_records
    if all_records:
        all_records = filter_records_with_company_websites(all_records)
        save_checkpoint(all_records, PARTIAL_OUTPUT_CSV)
        ai_verified_records = all_records

    # AI filtering disabled: go directly from website filtering to email enrichment.
    # if all_records and ENABLE_AI_FILTERING:
    #     print("\n[AI] Starting target-keyword verification...")
    #     try:
    #         ai_verified_records = apply_ai_filter_to_records(
    #             all_records,
    #             keywords=KEYWORDS,
    #             source_name=SOURCE_DIRECTORY,
    #             verified_csv=AI_VERIFIED_CSV,
    #             rejected_csv=AI_REJECTED_CSV,
    #             checkpoint_csv=AI_CHECKPOINT_CSV,
    #             batch_size=AI_BATCH_SIZE,
    #             concurrent=AI_CONCURRENT,
    #         )
    #     except Exception as e:
    #         print(f"[AI] Error: {e}")
    #         save_checkpoint(all_records, PARTIAL_OUTPUT_CSV)
    #         raise
    #     print(f"[AI] Keeping full checkpoint intact: {len(all_records)} total records.")
    #     print(f"[AI] Verified records are saved separately: {AI_VERIFIED_CSV} ({len(ai_verified_records)} records).")
    #     save_checkpoint(all_records, PARTIAL_OUTPUT_CSV)

    # Phase 3: Website email enrichment (NEW: requests-based)
    website_enrichment_records = all_records
    if website_enrichment_records and ENABLE_WEBSITE_EMAIL_ENRICHMENT:
        print("\n[WEBSITE] Starting website email enrichment for companies with websites...")
        try:
            enrich_emails_from_company_websites(
                website_enrichment_records,
                checkpoint_records=all_records,
                checkpoint_path=PARTIAL_OUTPUT_CSV,
            )
        except Exception as e:
            print(f"[WEBSITE] Error: {e}")
        save_checkpoint(all_records, PARTIAL_OUTPUT_CSV)

    # Final output
    df = pd.DataFrame([asdict(r) for r in all_records])
    df["name_norm"] = df["company_name"].str.lower().str.strip()
    df = df.drop_duplicates(subset=["name_norm"]).drop(columns=["name_norm"])
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    df_clean = df[df['email'].notna() & (df['email'] != '')]
    df_clean.to_csv(CLEANED_CSV, index=False, encoding="utf-8-sig")

    print(f"\n{'='*60}")
    print(f"DONE!")
    print(f"  Raw: {OUTPUT_CSV} ({len(df)} suppliers)")
    print(f"  With website: {(df['website_url'] != '').sum()}")
    print(f"  With email: {(df['email'] != '').sum()}")
    print(f"  Cleaned: {CLEANED_CSV} ({len(df_clean)} suppliers)")
    print(f"{'='*60}")

    reset_phase2_resume_index()
    print(f"[AUTH][RESUME] Reset {PHASE2_RESUME_STATE_FILE} to 0 after completed run.")

    return df


def main():
    parser = argparse.ArgumentParser(description="TradeWheel supplier scraper")
    parser.add_argument(
        "--resume-from-phase2",
        action="store_true",
        help="Load the existing partial CSV, skip Phase 1, then run Phase 2 and website email enrichment.",
    )
    parser.add_argument(
        "--resume-input",
        default=PARTIAL_OUTPUT_CSV,
        help="CSV input to use with --resume-from-phase2.",
    )
    args = parser.parse_args()

    df = scrape_tradewheel(
        resume_from_phase2=args.resume_from_phase2,
        resume_input=args.resume_input,
    )
    if df.empty:
        print("[INFO] No records found.")


if __name__ == "__main__":
    main()
