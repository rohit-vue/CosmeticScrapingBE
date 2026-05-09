"""
Kompass supplier scraper with integrated enrichment.
====================================================
Phase 1: Collect supplier records for cosmetic packaging terms
Phase 2: Enrich profile → website → email (requests-based, footer-first)
Output: Cleaned CSV with only email-enriched records

Features:
- Deduplication by company name
- Playwright browser fallback for anti-bot pages
- Proxy support (Webshare)
- Incremental checkpoint saves
- Concurrent email enrichment from company websites
- Footer-first email extraction using requests (same as EC21/Made-in-China)

FIX (2025-05): extract_external_website_from_profile now correctly reads
plain-text https:// links from the "Discover more on our Website" section
instead of the Kompass redirect anchor (which returned the same URL for
every company).
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from html import unescape
from threading import Lock
from typing import Any, Generator, Optional
from urllib.parse import quote_plus, urlparse
from urllib.request import ProxyHandler, Request, build_opener

import pandas as pd
import requests
from proxy_service import (
    ProxyEndpoint,
    ProxyExhaustedError,
    create_proxy_pool,
    fetch_with_proxy_rotation,
    goto_with_rotation,
    script_proxy_enabled,
)
from scraper_runtime_config import env_int, env_list

from scrapling.fetchers import StealthyFetcher

try:
    from playwright.sync_api import Page
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except ImportError:
    Page = None
    PlaywrightTimeoutError = Exception
    sync_playwright = None


# ===== KEYWORDS & COUNTRIES =====
KEYWORDS = [
    "cosmetic tubes",
    "cosmetic jars",
    "plastic packaging",
    "glass packaging",
    "lotion pumps",
    "airless pumps",
    "cosmetic packaging",
    "cosmetic bottles",
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
]

# ===== CONFIGURATION =====
SOURCE_DIRECTORY = "Kompass"
BASE_DOMAIN = "https://lb.kompass.com"
MAX_PAGES_PER_QUERY = 35
ZERO_NEW_PAGES_CUTOFF = 10
MIN_DELAY_SECONDS = 3
MAX_DELAY_SECONDS = 7
OUTPUT_CSV = "kompass_suppliers_phase1_raw.csv"
CLEANED_CSV = "kompass_suppliers_cleaned.csv"
PARTIAL_OUTPUT_CSV = "kompass_suppliers_phase1_partial.csv"
ENRICHED_CSV = "kompass_suppliers_enriched.csv"
CHECKPOINT_CSV = "kompass_suppliers_enrichment_checkpoint.csv"
TARGET_SUPPLIERS = 5000
AUTOSAVE_EVERY_NEW_RECORDS = 10
AUTOSAVE_EVERY_NEW_EMAILS = 10
PROFILE_ENRICH_DURING_SCRAPE = False
ENABLE_PROFILE_WEBSITE_ENRICHMENT = True
MAX_PROFILE_WEBSITE_LOOKUPS = TARGET_SUPPLIERS
ENABLE_WEBSITE_EMAIL_ENRICHMENT = True
MAX_WEBSITE_EMAIL_LOOKUPS = TARGET_SUPPLIERS

# ===== PHASE 2B: REQUEST-BASED EMAIL ENRICHMENT =====
WEBSITE_TIMEOUT = 15
WEBSITE_EMAIL_WORKERS = 10

CONTACT_PATHS = [
    "", "/contact", "/contact-us", "/contactus", "/contact.html",
    "/contact-us.html", "/contactus.html", "/about", "/about-us",
    "/about.html", "/about-us.html", "/contact/", "/contact-us/",
    "/contactus/", "/about/", "/about-us/", "/contactinfo",
    "/contact-info", "/contact_info", "/get-in-touch", "/reach-us",
    "/en/contact", "/en/contact-us",
]

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

JUNK_EMAIL_PHRASES = [
    "cloudflare", "404", "notfound", "blocked", "error",
    "ordercreditreport", "copyright", "@anytime", "@theforefront",
    "@homeandabroad", "@thistime", "@www", "pleasefeel",
]

ENABLE_BROWSER_FALLBACK = True
PLAYWRIGHT_HEADLESS = False
PLAYWRIGHT_CHANNEL = (
    os.environ["KOMPASS_PLAYWRIGHT_CHANNEL"].strip()
    if "KOMPASS_PLAYWRIGHT_CHANNEL" in os.environ
    else "chrome"
)
USE_PLAYWRIGHT_PROFILE = os.getenv("KOMPASS_P2_PROFILE_BROWSER", "1").strip().lower() not in {
    "0", "false", "no", "off",
}
PLAYWRIGHT_PROFILE_TIMEOUT_MS = int(os.getenv("KOMPASS_P2_PROFILE_TIMEOUT_MS", "90000") or "90000")
PLAYWRIGHT_ARGS = ["--disable-blink-features=AutomationControlled"]
COUNTRY_CODES = {
    "China": "CN",
    "South Korea": "KR",
    "Taiwan": "TW",
    "Japan": "JP",
    "Vietnam": "VN",
    "Thailand": "TH",
    "Singapore": "SG",
    "Malaysia": "MY",
    "Hong Kong": "HK",
}

KEYWORDS = env_list("SCRAPER_KEYWORDS", KEYWORDS)
COUNTRIES = env_list("SCRAPER_COUNTRIES", COUNTRIES)
TARGET_SUPPLIERS = env_int("SCRAPER_TARGET_SUPPLIERS", TARGET_SUPPLIERS)
MAX_PROFILE_WEBSITE_LOOKUPS = TARGET_SUPPLIERS
MAX_WEBSITE_EMAIL_LOOKUPS = TARGET_SUPPLIERS

# Proxy configuration
KOMPASS_USE_PROXY = script_proxy_enabled("kompass")
KOMPASS_USE_WEBSHARE = os.getenv("KOMPASS_USE_WEBSHARE", "1").strip().lower() not in {
    "0", "false", "off", "no",
}
KOMPASS_WEBSHARE_USER = os.getenv("KOMPASS_WEBSHARE_USER", "zwqoanas-rotate")
KOMPASS_WEBSHARE_PASS = os.getenv("KOMPASS_WEBSHARE_PASS", "3s52t6188b3p")
KOMPASS_WEBSHARE_HOST = os.getenv("KOMPASS_WEBSHARE_HOST", "p.webshare.io")
KOMPASS_WEBSHARE_PORT = os.getenv("KOMPASS_WEBSHARE_PORT", "80")
_KOMPASS_WEBSHARE_FETCH_LOGGED = False
_KOMPASS_WEBSHARE_IP_LOGGED = False
_KOMPASS_SCRAPLING_CONFIGURED = False

# Domains to skip when extracting external website
BLOCKED_DOMAINS = (
    # Kompass / platform
    "kompass.com", "ksales.ai",
    # Social
    "facebook.com", "instagram.com", "linkedin.com", "youtube.com",
    "wa.me", "twitter.com", "tiktok.com", "pinterest.com", "x.com",
    # Analytics / CDN / ads — these caused the duplicate-URL bug via Priority-5
    "google.com", "googletagmanager.com", "googlesyndication.com",
    "googleadservices.com", "googleapis.com", "gstatic.com",
    "doubleclick.net", "ggpht.com",
    "cloudflare.com", "cloudflareinsights.com",
    "jquery.com", "jsdelivr.net", "bootstrapcdn.com",
    "unpkg.com", "cdnjs.cloudflare.com",
    "axept.io", "axeptio.eu",
    "cookiebot.com", "onetrust.com",
    "hotjar.com", "mouseflow.com", "clarity.ms",
    "hubspot.com", "hubspotusercontent.com",
    "intercomcdn.com", "zendesk.com",
    "smart-data-systems.com",
    "w3.org", "schema.org", "opengraph.io",
)

BLOCKED_EMAIL_TOKENS = [
    "kompass.com", "alibaba.com", "made-in-china.com", "example.com",
    "cloudflare", "404", "notfound", "blocked", "error",
    "copyright", "@anytime", "@theforefront", "@homeandabroad",
    "@thistime", "nobody@",
]

PROXY_POOL = None
if KOMPASS_USE_PROXY and not KOMPASS_USE_WEBSHARE:
    PROXY_POOL = create_proxy_pool("kompass")


# ===== PROXY HELPERS =====

def _kompass_webshare_proxy_url() -> str:
    return (
        f"http://{KOMPASS_WEBSHARE_USER}:{KOMPASS_WEBSHARE_PASS}"
        f"@{KOMPASS_WEBSHARE_HOST}:{KOMPASS_WEBSHARE_PORT}/"
    )


def _kompass_webshare_playwright_proxy() -> dict[str, str]:
    return {
        "server": f"http://{KOMPASS_WEBSHARE_HOST}:{KOMPASS_WEBSHARE_PORT}",
        "username": KOMPASS_WEBSHARE_USER,
        "password": KOMPASS_WEBSHARE_PASS,
    }


def _kompass_webshare_fetch_html(url: str, headers: dict[str, str], timeout: int = 40) -> tuple[int, str]:
    proxy_url = _kompass_webshare_proxy_url()
    opener = build_opener(
        ProxyHandler({"http": proxy_url, "https": proxy_url})
    )
    req = Request(url, headers=headers)
    with opener.open(req, timeout=timeout) as resp:
        body = resp.read()
        status = int(getattr(resp, "status", 200) or 200)
    return status, body.decode("utf-8", errors="ignore")


def _log_kompass_webshare_ip_once() -> None:
    global _KOMPASS_WEBSHARE_IP_LOGGED
    if _KOMPASS_WEBSHARE_IP_LOGGED or not KOMPASS_USE_PROXY or not KOMPASS_USE_WEBSHARE:
        return
    try:
        status, text = _kompass_webshare_fetch_html(
            "https://ipv4.webshare.io/",
            headers={"User-Agent": DEFAULT_HEADERS["User-Agent"]},
        )
        line = (text or "").strip().splitlines()
        sample = line[0][:200] if line else "(empty response)"
        print(f"[PROXY][WEBSHARE][KOMPASS] exit-ip check status={status} response={sample}")
    except Exception as exc:
        print(f"[PROXY][WEBSHARE][KOMPASS][WARN] exit-ip check failed: {exc}")
    _KOMPASS_WEBSHARE_IP_LOGGED = True


def _log_kompass_proxy_mode_once() -> None:
    if not KOMPASS_USE_PROXY:
        print("[PROXY][KOMPASS] proxy disabled (KOMPASS_USE_PROXY=0 or SCRAPER_PROXY_ENABLED=0)")
        return
    if KOMPASS_USE_WEBSHARE:
        print(
            f"[PROXY][WEBSHARE][KOMPASS] enabled "
            f"(host={KOMPASS_WEBSHARE_HOST}:{KOMPASS_WEBSHARE_PORT}, "
            f"user={KOMPASS_WEBSHARE_USER})"
        )
    else:
        print("[PROXY][WEBSHARE][KOMPASS] inline Webshare off; using shared Webshare pool via proxy_service")


def _configure_scrapling_once() -> None:
    global _KOMPASS_SCRAPLING_CONFIGURED
    if _KOMPASS_SCRAPLING_CONFIGURED:
        return
    cfg: dict[str, object] = {}
    env_map = {
        "adaptive": "KOMPASS_SCRAPLING_ADAPTIVE",
        "huge_tree": "KOMPASS_SCRAPLING_HUGE_TREE",
        "keep_comments": "KOMPASS_SCRAPLING_KEEP_COMMENTS",
        "keep_cdata": "KOMPASS_SCRAPLING_KEEP_CDATA",
        "adaptive_domain": "KOMPASS_SCRAPLING_ADAPTIVE_DOMAIN",
        "storage": "KOMPASS_SCRAPLING_STORAGE",
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
            print(f"[SCRAPLING][KOMPASS] configured parser options: {tuple(cfg.keys())}")
        except Exception as exc:
            print(f"[SCRAPLING][WARN] StealthyFetcher.configure failed: {exc}")
    else:
        print("[SCRAPLING][KOMPASS] using default parser configuration")
    _KOMPASS_SCRAPLING_CONFIGURED = True


def _kompass_fetch_validator(html: str) -> bool:
    h = html.lower()
    return ("kompass" in h) and (
        "companysearch" in h or "supplier" in h or "searchcompanies" in h
    )


def _kompass_browser_validator(html: str) -> bool:
    return _kompass_fetch_validator(html)


# ===== BROWSER HOLDER =====

class KompassBrowserHolder:
    """Playwright browser + proxy endpoint; supports relaunch for goto_with_rotation."""

    def __init__(self) -> None:
        self.playwright = None
        self.browser = None
        self.context = None
        self.page: Optional[Page] = None
        self.endpoint: Optional[ProxyEndpoint] = None

    def relaunch(self, ep: Optional[ProxyEndpoint]) -> Page:
        assert self.playwright is not None
        if self.context:
            try:
                self.context.close()
            except Exception:
                pass
        if self.browser:
            try:
                self.browser.close()
            except Exception:
                pass
        launch_kwargs: dict = {
            "headless": PLAYWRIGHT_HEADLESS,
            "args": PLAYWRIGHT_ARGS,
        }
        if PLAYWRIGHT_CHANNEL:
            launch_kwargs["channel"] = PLAYWRIGHT_CHANNEL
        if KOMPASS_USE_PROXY and KOMPASS_USE_WEBSHARE:
            launch_kwargs["proxy"] = _kompass_webshare_playwright_proxy()
            print(
                f"[PROXY][WEBSHARE][KOMPASS] Playwright relaunch using "
                f"{KOMPASS_WEBSHARE_HOST}:{KOMPASS_WEBSHARE_PORT}"
            )
        elif PROXY_POOL:
            cfg = PROXY_POOL.playwright_config(ep)
            if cfg:
                launch_kwargs["proxy"] = cfg
        try:
            self.browser = self.playwright.chromium.launch(**launch_kwargs)
        except Exception as exc:
            if launch_kwargs.pop("channel", None):
                print(f"[BROWSER][WARN] Could not launch channel '{PLAYWRIGHT_CHANNEL}': {exc}")
                self.browser = self.playwright.chromium.launch(**launch_kwargs)
            else:
                raise
        self.context = self.browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            user_agent=DEFAULT_HEADERS["User-Agent"],
        )
        browser_name = PLAYWRIGHT_CHANNEL if PLAYWRIGHT_CHANNEL else "chromium"
        print(f"[BROWSER][KOMPASS] relaunch browser={browser_name}")
        self.page = self.context.new_page()
        self.endpoint = ep
        return self.page


# ===== DATA CLASSES =====

@dataclass
class SupplierRecord:
    company_name: str
    website_url: str
    country: str
    source_directory: str = SOURCE_DIRECTORY
    email: str = ""
    profile_url: str = ""


class StreamingCsvSink:
    """Append records immediately so progress is durable during long runs."""

    def __init__(self, output_paths: list[str], fieldnames: list[str]) -> None:
        self.output_paths = output_paths
        self.fieldnames = fieldnames
        self._lock = Lock()

    def append(self, record: SupplierRecord) -> None:
        row = asdict(record)
        with self._lock:
            for path in self.output_paths:
                exists = os.path.exists(path) and os.path.getsize(path) > 0
                with open(path, "a", newline="", encoding="utf-8-sig") as fh:
                    writer = csv.DictWriter(fh, fieldnames=self.fieldnames)
                    if not exists:
                        writer.writeheader()
                    writer.writerow(row)

    def rewrite_full(self, records: list[SupplierRecord]) -> None:
        rows = [asdict(r) for r in records]
        with self._lock:
            for path in self.output_paths:
                with open(path, "w", newline="", encoding="utf-8-sig") as fh:
                    writer = csv.DictWriter(fh, fieldnames=self.fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)


class CsvStore:
    """Thread-safe incremental CSV writer + full rewrite for enrichment phase."""

    def __init__(self, path: str, fieldnames: list[str] | None = None) -> None:
        self.path = path
        self.fieldnames = fieldnames or FIELDNAMES
        self._lock = Lock()

    def write_all(self, rows: list[dict]) -> None:
        with self._lock:
            with open(self.path, "w", newline="", encoding="utf-8-sig") as fh:
                writer = csv.DictWriter(fh, fieldnames=self.fieldnames)
                writer.writeheader()
                writer.writerows(rows)

    def read_all(self) -> list[dict]:
        if not os.path.exists(self.path):
            return []
        with open(self.path, newline="", encoding="utf-8-sig") as fh:
            return list(csv.DictReader(fh))


FIELDNAMES = ["company_name", "website_url", "country", "source_directory", "email", "profile_url"]


# ===== UTILITY FUNCTIONS =====

def random_delay(min_sec: float = MIN_DELAY_SECONDS, max_sec: float = MAX_DELAY_SECONDS) -> None:
    time.sleep(random.uniform(min_sec, max_sec))


def strip_tags(text: str) -> str:
    if not text:
        return ""
    no_tags = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", unescape(no_tags)).strip()


def normalize_url(url: str, base: str = BASE_DOMAIN) -> str:
    value = (url or "").strip()
    if not value:
        return ""
    if value.startswith(("http://", "https://")):
        return value
    if value.startswith("//"):
        return f"https:{value}"
    if value.startswith("/"):
        return f"{base}{value}"
    return f"{base}/{value.lstrip('/')}"


def is_plausible_external_website(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.netloc or "").lower()
    if not host:
        return False
    return not any(token in host for token in BLOCKED_DOMAINS)


def is_blocked_platform_website_url(url: str) -> bool:
    return not is_plausible_external_website((url or "").strip())


def clean_email(email: str) -> str:
    if not email:
        return ""
    match = re.match(r'([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})([A-Z].*)?', email)
    return match.group(1) if match else email


def is_useful_email(email: str) -> bool:
    if not email or len(email) > 80 or len(email) < 6:
        return False
    if ' ' in email or email.count('@') != 1:
        return False
    if not re.search(r'\.(com|net|org|cn|kr|jp|tw|vn|th|sg|my|hk|co\.\w+)$', email, re.I):
        return False
    low = email.lower()
    if any(b in low for b in BLOCKED_EMAIL_TOKENS):
        return False
    if any(j in low for j in JUNK_EMAIL_PHRASES):
        return False
    return True


def initialize_fetcher() -> Any:
    _configure_scrapling_once()
    return StealthyFetcher


# ===== FETCH FUNCTIONS =====

def fetch_html(fetcher: Any, url: str, timeout: int = 40) -> tuple[int, str]:
    global _KOMPASS_WEBSHARE_FETCH_LOGGED
    if KOMPASS_USE_PROXY and KOMPASS_USE_WEBSHARE:
        if not _KOMPASS_WEBSHARE_FETCH_LOGGED:
            host = urlparse(url).netloc or "unknown"
            print(f"[PROXY][WEBSHARE][KOMPASS] HTTP fetch active (first host={host})")
            _KOMPASS_WEBSHARE_FETCH_LOGGED = True
        _log_kompass_webshare_ip_once()
        try:
            return _kompass_webshare_fetch_html(url, headers=DEFAULT_HEADERS, timeout=timeout)
        except Exception as exc:
            print(f"[PROXY][WEBSHARE][WARN] Request failed: {exc}")
            return 0, ""

    try:
        response = fetch_with_proxy_rotation(
            fetcher=fetcher,
            url=url,
            headers=DEFAULT_HEADERS,
            pool=PROXY_POOL if KOMPASS_USE_PROXY else None,
            validator=_kompass_fetch_validator,
        )
    except ProxyExhaustedError as exc:
        print(f"[PROXY][WARN] {exc}")
        return 0, ""
    status_code = getattr(response, "status", None) or getattr(response, "status_code", 200)
    body = getattr(response, "body", None)
    if isinstance(body, (bytes, bytearray)):
        return int(status_code), body.decode("utf-8", errors="ignore")
    if isinstance(body, str) and body:
        return int(status_code), body
    if hasattr(response, "text") and response.text:
        return int(status_code), response.text
    return int(status_code), str(response)


def fetch_page_requests(url: str) -> Optional[str]:
    """Fetch with requests for email enrichment phase. Returns HTML or None."""
    try:
        resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=WEBSITE_TIMEOUT, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
        return None
    except Exception:
        return None


def fetch_html_simple(url: str, timeout: int = 30) -> tuple[int, str]:
    """Simple HTTP fetch with optional proxy (for enrichment phase)."""
    headers = DEFAULT_HEADERS.copy()
    try:
        if KOMPASS_USE_PROXY and KOMPASS_USE_WEBSHARE:
            return _kompass_webshare_fetch_html(url, headers=headers, timeout=timeout)
        else:
            import urllib.request
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
                status = int(resp.status)
            return status, body.decode("utf-8", errors="ignore")
    except Exception as exc:
        print(f"    [FETCH ERROR] {url[:80]} → {exc}")
        return 0, ""


def fetch_html_with_browser_fallback(
    fetcher: Any,
    url: str,
    browser_page: Optional[Page] = None,
    *,
    browser_holder: Optional[KompassBrowserHolder] = None,
) -> tuple[int, str]:
    status_code = 0
    html = ""
    try:
        status_code, html = fetch_html(fetcher, url)
    except Exception:
        status_code, html = 0, ""

    if status_code == 200 and html:
        return status_code, html

    active_page = browser_holder.page if browser_holder else browser_page
    if not ENABLE_BROWSER_FALLBACK or active_page is None:
        return status_code, html

    def accept_kompass_cookies(page: Page) -> None:
        selectors = (
            "button#axeptio_btn_acceptAll",
            "button:has-text('I accept')",
            "button:has-text('Accept')",
            "#axeptio_btn_acceptAll",
        )
        try:
            frames = page.frames
        except Exception:
            frames = []
        for frame in frames:
            for selector in selectors:
                try:
                    btn = frame.locator(selector).first
                    if btn.count() == 0:
                        continue
                    if btn.is_visible(timeout=1500):
                        btn.click(timeout=3000)
                    else:
                        btn.click(timeout=3000, force=True)
                    page.wait_for_timeout(600)
                    return
                except Exception:
                    continue
        for frame in frames:
            try:
                btn = frame.get_by_role("button", name=re.compile(r"i\s*accept|accept all|accept", re.I)).first
                if btn.count() > 0:
                    if btn.is_visible(timeout=1200):
                        btn.click(timeout=3000)
                    else:
                        btn.click(timeout=3000, force=True)
                    page.wait_for_timeout(600)
                    return
            except Exception:
                continue

    def close_kompass_help_modal(page: Page) -> None:
        selectors = (
            "button.close[data-dismiss='modal']",
            ".modal-header button.close",
            ".modal-header .close",
        )
        for selector in selectors:
            try:
                close_btn = page.locator(selector).first
                if close_btn.count() == 0:
                    continue
                if close_btn.is_visible(timeout=1200):
                    close_btn.click(timeout=2500)
                    page.wait_for_timeout(400)
                    return
            except Exception:
                continue

    try:
        try:
            active_page.goto("about:blank", wait_until="domcontentloaded", timeout=15000)
        except Exception:
            pass

        browser_status = 200
        if PROXY_POOL and browser_holder is not None:

            def _relaunch(ep: Optional[ProxyEndpoint]) -> Page:
                return browser_holder.relaunch(ep)

            try:
                new_page, new_ep = goto_with_rotation(
                    active_page,
                    url,
                    PROXY_POOL,
                    _relaunch,
                    current_endpoint=browser_holder.endpoint,
                    timeout_ms=90000,
                    wait_until="domcontentloaded",
                    validate=_kompass_browser_validator,
                )
                browser_holder.page = new_page
                browser_holder.endpoint = new_ep
                active_page = new_page
            except ProxyExhaustedError as exc:
                print(f"[PROXY][WARN] {exc}")
                return status_code, html
        else:
            response = active_page.goto(url, wait_until="domcontentloaded", timeout=90000)
            browser_status = response.status if response is not None else 200
            if browser_status != 200:
                return status_code, html

        accept_kompass_cookies(active_page)
        close_kompass_help_modal(active_page)
        try:
            active_page.wait_for_load_state("networkidle", timeout=12000)
        except Exception:
            pass

        try:
            active_page.wait_for_selector(
                "div[id^='result-'], .noresult, .no-results, .empty-result",
                timeout=10000,
                state="attached",
            )
        except Exception:
            pass

        try:
            for _ in range(4):
                active_page.mouse.wheel(0, 2400)
                active_page.wait_for_timeout(450)
            active_page.evaluate("window.scrollTo(0, 0)")
        except Exception:
            pass
        active_page.wait_for_timeout(900)
        rendered_html = active_page.content()
        if rendered_html:
            return browser_status, rendered_html
    except PlaywrightTimeoutError:
        return status_code, html
    except Exception:
        return status_code, html
    return status_code, html


# ===== ENRICHMENT PAGE FETCH (Phase 2A) =====

@contextmanager
def profile_playwright_page() -> Generator[Any, None, None]:
    """One browser + context + page for the profile→website loop."""
    if sync_playwright is None:
        raise RuntimeError(
            "playwright is not installed. Run: pip install playwright && playwright install chrome"
        )
    pw = sync_playwright().start()
    browser = None
    try:
        launch_kwargs: dict[str, Any] = {
            "headless": PLAYWRIGHT_HEADLESS,
            "args": PLAYWRIGHT_ARGS,
        }
        if PLAYWRIGHT_CHANNEL:
            launch_kwargs["channel"] = PLAYWRIGHT_CHANNEL
        if KOMPASS_USE_PROXY and KOMPASS_USE_WEBSHARE:
            launch_kwargs["proxy"] = _kompass_webshare_playwright_proxy()
        elif PROXY_POOL:
            pass
        try:
            browser = pw.chromium.launch(**launch_kwargs)
        except Exception as exc:
            if launch_kwargs.pop("channel", None):
                print(f"[PROFILE][WARN] Could not launch channel '{PLAYWRIGHT_CHANNEL}': {exc}")
                browser = pw.chromium.launch(**launch_kwargs)
            else:
                raise
        context = browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            user_agent=DEFAULT_HEADERS["User-Agent"],
        )
        page = context.new_page()
        yield page
    finally:
        try:
            if browser:
                browser.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass


def fetch_profile_html_playwright(page: Any, url: str) -> tuple[int, str, str]:
    """
    Load a Kompass profile in the shared Playwright page.

    Returns (status, html, website_url) where website_url is extracted
    directly from the live DOM — far more reliable than regex on raw HTML.

    DOM extraction order:
      A) Read all <a> hrefs inside the "Discover more on our Website" block
         and the presentation section — these are plain anchor tags with the
         real company URL.
      B) JavaScript evaluation that walks every <a> in the page and returns
         hrefs that look like real external company websites.
      C) Falls back to raw HTML for regex parsing in extract_external_website_from_profile.
    """
    website_url = ""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_PROFILE_TIMEOUT_MS)
        page.wait_for_timeout(1500)

        # --- Cookie consent ---
        cookie_selectors = [
            "button:has-text('Continue without consent')",
            "button:has-text('Continue without accepting')",
            "button#axeptio_btn_acceptAll",
            "button#axeptio_btn_acceptAllAndContinue",
            "button:has-text('Accept all')",
            "button:has-text('I accept')",
            "button:has-text('Accept')",
            "button:has-text('Let me choose')",
        ]

        cookie_handled = False
        for selector in cookie_selectors:
            try:
                btn = page.locator(selector).first
                if btn.count() > 0 and btn.is_visible(timeout=2000):
                    btn.click(timeout=3000)
                    page.wait_for_timeout(800)
                    print(f"    [COOKIE] Handled: {selector}")
                    cookie_handled = True
                    break
            except Exception:
                continue

        if not cookie_handled:
            try:
                frames = page.frames
                for frame in frames:
                    for selector in cookie_selectors:
                        try:
                            btn = frame.locator(selector).first
                            if btn.count() > 0 and btn.is_visible(timeout=1500):
                                btn.click(timeout=3000)
                                page.wait_for_timeout(800)
                                print(f"    [COOKIE] Handled in iframe: {selector}")
                                cookie_handled = True
                                break
                        except Exception:
                            continue
                    if cookie_handled:
                        break
            except Exception:
                pass

        # --- Modal / popup close ---
        modal_handled = False
        modal_selectors = [
            "button.close[data-dismiss='modal']",
            ".modal-header button.close",
            ".modal-header .close",
            "button[aria-label='Close']",
            "button:has-text('Close')",
            "button:has-text('×')",
            ".modal .close",
            "[data-dismiss='modal']",
        ]

        for selector in modal_selectors:
            try:
                close_btn = page.locator(selector).first
                if close_btn.count() > 0 and close_btn.is_visible(timeout=1500):
                    close_btn.click(timeout=2500)
                    page.wait_for_timeout(400)
                    print(f"    [MODAL] Closed via: {selector}")
                    modal_handled = True
                    break
            except Exception:
                continue

        if not modal_handled:
            try:
                backdrop = page.locator('.modal-backdrop, .modal.show, [role="dialog"]').first
                if backdrop.count() > 0 and backdrop.is_visible(timeout=1000):
                    page.keyboard.press("Escape")
                    page.wait_for_timeout(500)
                    print("    [MODAL] Dismissed via Escape key")
                    modal_handled = True
            except Exception:
                pass

        if not modal_handled:
            try:
                page.keyboard.press("Escape")
                page.wait_for_timeout(300)
            except Exception:
                pass

        # --- Scroll to trigger lazy-loaded content ---
        try:
            for _ in range(3):
                page.mouse.wheel(0, 1500)
                page.wait_for_timeout(400)
            page.evaluate("window.scrollTo(0, 0)")
        except Exception:
            pass

        # --- Wait for About Us / website section to render ---
        page.wait_for_timeout(2000)
        try:
            page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass

        # ----------------------------------------------------------------
        # DIRECT DOM EXTRACTION — most reliable approach.
        # We use JavaScript to walk the live DOM and collect every <a href>
        # that points to a real external company website.
        # Blocked domains are passed in so the JS can filter them out.
        # ----------------------------------------------------------------
        blocked_js = list(BLOCKED_DOMAINS)
        try:
            website_url = page.evaluate(
                """(blockedDomains) => {
                    function isBlocked(href) {
                        if (!href || !href.startsWith('http')) return true;
                        try {
                            var host = new URL(href).hostname.toLowerCase();
                            return blockedDomains.some(function(d) {
                                return host === d || host.endsWith('.' + d);
                            });
                        } catch(e) { return true; }
                    }

                    // Strategy A: links inside the "Discover more" / presentation block
                    var presentationSelectors = [
                        '[class*="presentation"]',
                        '[id*="presentation"]',
                        '[class*="about"]',
                        '[class*="description"]',
                        '[class*="website"]',
                        '.trWebSite',
                    ];
                    for (var i = 0; i < presentationSelectors.length; i++) {
                        var containers = document.querySelectorAll(presentationSelectors[i]);
                        for (var j = 0; j < containers.length; j++) {
                            var links = containers[j].querySelectorAll('a[href]');
                            for (var k = 0; k < links.length; k++) {
                                var href = links[k].getAttribute('href') || '';
                                if (!isBlocked(href)) return href;
                            }
                            // Also check plain text URLs inside the container
                            var text = containers[j].innerText || '';
                            var match = text.match(/https?:\\/\\/[^\\s]{6,}/);
                            if (match) {
                                var candidate = match[0].replace(/[.,;)\\]]+$/, '');
                                if (!isBlocked(candidate)) return candidate;
                            }
                        }
                    }

                    // Strategy B: the dedicated webSite_presentation anchor
                    var wsAnchors = document.querySelectorAll('[id^="webSite_presentation_"]');
                    for (var i = 0; i < wsAnchors.length; i++) {
                        var href = wsAnchors[i].getAttribute('href') || '';
                        // Skip Kompass relay URLs
                        if (href && href.indexOf('mise-en-relation') === -1 && !isBlocked(href)) {
                            return href;
                        }
                        // If it IS a relay, get the displayed text which is often the real URL
                        var txt = (wsAnchors[i].innerText || '').trim();
                        if (txt.startsWith('http') && !isBlocked(txt)) return txt;
                        // Or look for a data attribute
                        var dataUrl = wsAnchors[i].getAttribute('data-url')
                                   || wsAnchors[i].getAttribute('data-href')
                                   || wsAnchors[i].getAttribute('data-website');
                        if (dataUrl && !isBlocked(dataUrl)) return dataUrl;
                    }

                    // Strategy C: Company website button (top of page)
                    var btns = document.querySelectorAll('a.btn, a[class*="website"], a[href*="webSite"]');
                    for (var i = 0; i < btns.length; i++) {
                        var href = btns[i].getAttribute('href') || '';
                        if (href && href.indexOf('mise-en-relation') === -1 && !isBlocked(href)) {
                            return href;
                        }
                    }

                    // Strategy D: Walk every <a> in page order, skip blocked
                    var all = document.querySelectorAll('a[href]');
                    for (var i = 0; i < all.length; i++) {
                        var href = all[i].getAttribute('href') || '';
                        if (href.startsWith('http') && href.indexOf('mise-en-relation') === -1 && !isBlocked(href)) {
                            return href;
                        }
                    }

                    return '';
                }""",
                blocked_js,
            )
            if website_url:
                print(f"    [DOM-EXTRACT] Found via JS: {website_url[:70]}")
        except Exception as exc:
            print(f"    [DOM-EXTRACT] JS evaluation failed: {exc}")
            website_url = ""

        html = page.content()
        if not html:
            return 0, "", ""
        return 200, html, website_url or ""

    except PlaywrightTimeoutError:
        print(f"    [PLAYWRIGHT TIMEOUT] {url[:80]}")
        return 0, "", ""
    except Exception as exc:
        print(f"    [PLAYWRIGHT ERROR] {url[:80]} → {exc}")
        return 0, "", ""


# ===== PHASE 2A: PROFILE → WEBSITE =====

def _extract_anchors(html: str) -> list[tuple[str, str]]:
    results = []
    for href, inner in re.findall(
        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        html,
        flags=re.I | re.S,
    ):
        results.append((normalize_url(href.strip()), strip_tags(inner)))
    return results


def extract_external_website_from_profile(profile_html: str) -> str:
    """
    Regex fallback for when Playwright DOM extraction returns nothing.

    The key insight from the bug investigation:
    - Kompass renders company websites as <a href="..."> anchors inside the
      About Us / presentation block — NOT as plain text.
    - The old regex used [^<]{0,300} which stopped at the first HTML tag,
      so it never reached the href inside the <a>.
    - Priority 5 (full-page URL scan) then fired and returned Google/CDN URLs.

    Fixed strategy:
    1.  Extract hrefs from <a> tags inside presentation/about/website sections.
    2.  Extract hrefs from the webSite_presentation anchor (skip Kompass relays).
    3.  Extract hrefs from the trWebSite table row.
    4.  Plain-text URL scan of the whole page, with expanded blocked-domain list.
    """

    def _first_good_href(html_fragment: str) -> str:
        """Return first external href from <a> tags in the fragment."""
        for href, _ in _extract_anchors(html_fragment):
            href = href.rstrip(".,;)")
            if (
                is_plausible_external_website(href)
                and "mise-en-relation" not in href
            ):
                return href
        return ""

    # --- PRIORITY 1: <a> links inside presentation / about / website sections ---
    # These CSS-class fragments contain the "Discover more on our Website" links.
    section_patterns = [
        r'(<(?:div|section|td)[^>]*class=["\'][^"\']*(?:presentation|about|website|description|contact)[^"\']*["\'][^>]*>.*?</(?:div|section|td)>)',
        r'(<(?:div|section)[^>]*id=["\'][^"\']*(?:presentation|about|website)[^"\']*["\'][^>]*>.*?</(?:div|section)>)',
    ]
    for pattern in section_patterns:
        for fragment in re.findall(pattern, profile_html, flags=re.I | re.S):
            result = _first_good_href(fragment)
            if result:
                print(f"    [WEBSITE-EXTRACT] Priority-1 (section anchor): {result[:70]}")
                return result

    # --- PRIORITY 2: webSite_presentation_N anchor ---
    # Extract href and also the inner text (which is sometimes the real URL).
    for m in re.finditer(
        r'<a[^>]*id="webSite_presentation_\d+"[^>]*href="([^"]*)"[^>]*>(.*?)</a>',
        profile_html,
        flags=re.I | re.S,
    ):
        href = m.group(1).strip().rstrip(".,;)")
        inner_text = strip_tags(m.group(2)).strip().rstrip(".,;)")

        # The inner text is often the actual company URL displayed to the user
        for candidate in [inner_text, href]:
            if not candidate:
                continue
            # Ensure it starts with http if it looks like a domain
            if not candidate.startswith("http") and re.match(r'^www\.[a-z]', candidate, re.I):
                candidate = "http://" + candidate
            if (
                candidate.startswith("http")
                and "mise-en-relation" not in candidate
                and is_plausible_external_website(candidate)
            ):
                print(f"    [WEBSITE-EXTRACT] Priority-2 (webSite anchor): {candidate[:70]}")
                return candidate

    # --- PRIORITY 3: trWebSite table row ---
    row_match = re.search(
        r'<tr[^>]*class=["\'][^"\']*\btrWebSite\b[^"\']*["\'][^>]*>(.*?)</tr>',
        profile_html,
        flags=re.I | re.S,
    )
    if row_match:
        result = _first_good_href(row_match.group(1))
        if result:
            print(f"    [WEBSITE-EXTRACT] Priority-3 (trWebSite row): {result[:70]}")
            return result

    # --- PRIORITY 4: ALL <a href> anchors in page order, skip blocked ---
    # Walk every anchor — more reliable than plain-text URL scanning.
    for href, label in _extract_anchors(profile_html):
        href = href.rstrip(".,;)")
        if (
            is_plausible_external_website(href)
            and "mise-en-relation" not in href
        ):
            print(f"    [WEBSITE-EXTRACT] Priority-4 (all anchors): {href[:70]}")
            return href

    # --- PRIORITY 5: Plain-text URL scan (last resort, BLOCKED_DOMAINS guards CDNs) ---
    all_urls = re.findall(r'https?://[^\s<"\'>\)\]]{6,}', profile_html)
    for raw_url in all_urls:
        u = raw_url.rstrip(".,;)")
        if is_plausible_external_website(u) and "mise-en-relation" not in u:
            print(f"    [WEBSITE-EXTRACT] Priority-5 (plain-text scan): {u[:70]}")
            return u

    print("    [WEBSITE-EXTRACT] No external website found.")
    return ""


def resolve_redirect_url(url: str) -> str:
    """
    Follow HTTP redirects to resolve the final destination URL.
    Previously only handled 'mise-en-relation' URLs; now always resolves
    so we catch any Kompass-style redirect regardless of path pattern.
    """
    if not url:
        return url
    try:
        resp = requests.get(
            url,
            headers=DEFAULT_HEADERS,
            timeout=15,
            allow_redirects=True,
        )
        final_url = resp.url
        if is_plausible_external_website(final_url):
            if final_url != url:
                print(f"    [REDIRECT] {url[:50]} → {final_url[:60]}")
            return final_url
    except Exception:
        pass
    return url


def enrich_website_from_profile_rows(
    rows: list[dict],
    store: CsvStore,
    max_lookups: int = 0,
) -> list[dict]:
    """For every row where website_url is empty and profile_url is set,
    fetch the Kompass profile and extract the external website URL."""
    candidates = [
        r for r in rows
        if not (r.get("website_url") or "").strip()
        and (r.get("profile_url") or "").strip()
    ]

    limit = max_lookups if max_lookups > 0 else len(candidates)
    candidates = candidates[:limit]

    total = len(candidates)
    if total == 0:
        print("[PROFILE] All rows already have website_url — skipping profile enrichment.")
        return rows

    print(f"\n[PROFILE] Enriching website_url for {total} rows from Kompass profiles...")

    use_browser = USE_PLAYWRIGHT_PROFILE and sync_playwright is not None
    browser_cm: Any = None
    pw_page: Any = None
    if use_browser:
        try:
            browser_cm = profile_playwright_page()
            pw_page = browser_cm.__enter__()
            print(
                f"[PROFILE] Using Playwright (channel={PLAYWRIGHT_CHANNEL or 'chromium'}, "
                f"headless={PLAYWRIGHT_HEADLESS}, proxy={'ON' if KOMPASS_USE_PROXY else 'OFF'})."
            )
        except Exception as exc:
            print(f"[PROFILE][WARN] Playwright unavailable ({exc}); using HTTP fetch_html fallback.")
            pw_page = None
            browser_cm = None

    updated = 0
    try:
        for idx, row in enumerate(candidates, 1):
            profile_url = row["profile_url"].strip()
            print(f"  [{idx}/{total}] {row['company_name'][:55]:<55} ", end="", flush=True)

            if pw_page is not None:
                status, html, dom_website = fetch_profile_html_playwright(pw_page, profile_url)
            else:
                fetcher = initialize_fetcher()
                status, html = fetch_html(fetcher, profile_url)
                dom_website = ""

            if status != 200 or not html:
                label = "browser" if pw_page is not None else "HTTP"
                print(f"✗ ({label} status {status})")
                random_delay(min_sec=2, max_sec=5)
                continue

            # Prefer direct DOM extraction; fall back to regex on raw HTML
            website = dom_website or extract_external_website_from_profile(html)

            if website:
                # Always follow redirects — catches Kompass relay URLs
                resolved = resolve_redirect_url(website)
                if resolved and is_plausible_external_website(resolved):
                    website = resolved
                row["website_url"] = website
                updated += 1
                print(f"✓  {website[:60]}")
            else:
                print("– (no website found)")

            if idx % AUTOSAVE_EVERY_NEW_RECORDS == 0:
                store.write_all(rows)
                print(f"  [CHECKPOINT] Saved after {idx} profile lookups ({updated} updated).")

            random_delay(min_sec=2, max_sec=5)
    finally:
        if browser_cm is not None:
            try:
                browser_cm.__exit__(None, None, None)
            except Exception:
                pass

    store.write_all(rows)
    print(f"\n[PROFILE] Done. {updated}/{total} rows got website_url.")
    return rows


# ===== PHASE 2B: WEBSITE → EMAIL =====

def extract_email_from_html(html: str) -> Optional[str]:
    """Extract email from HTML — checks footer first, then full page."""
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
            email = _find_email_in_text(match.group(1))
            if email:
                return email

    return _find_email_in_text(text)


def _find_email_in_text(text: str) -> Optional[str]:
    """Find valid email in text."""
    text_lower = text.lower()

    mailto = re.search(
        r"mailto:([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})",
        text_lower, re.I,
    )
    if mailto:
        email = mailto.group(1)
        if is_useful_email(email):
            return email

    clean = re.sub(r"<[^>]+>", " ", text_lower)
    for pat, rep in [
        (r"\s*\(at\)\s*", "@"), (r"\s*\[at\]\s*", "@"),
        (r"\s+at\s+", "@"), (r"\s*\(dot\)\s*", "."),
        (r"\s*\[dot\]\s*", "."), (r"\s+dot\s+", "."),
    ]:
        clean = re.sub(pat, rep, clean, flags=re.I)
    clean = re.sub(r"\s+", "", clean)

    match = re.search(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", clean)
    if match:
        email = match.group(0)
        if is_useful_email(email):
            return email
    return None


def lookup_email_requests(website_url: str) -> str:
    """Try multiple contact page URLs until an email is found."""
    if not website_url or str(website_url) == "nan" or website_url == "":
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


def enrich_email_from_website_rows(
    rows: list[dict],
    store: CsvStore,
    max_lookups: int = 0,
    max_workers: int = 10,
) -> list[dict]:
    """
    For every row with website_url but no email, visit the website and
    hunt for an email address (concurrently with requests).
    """
    candidates = [
        r for r in rows
        if (r.get("website_url") or "").strip()
        and not (r.get("email") or "").strip()
        and not is_blocked_platform_website_url((r.get("website_url") or "").strip())
    ]

    limit = max_lookups if max_lookups > 0 else len(candidates)
    candidates = candidates[:limit]

    total = len(candidates)
    if total == 0:
        print("[EMAIL] No rows need email enrichment — skipping.")
        return rows

    print(f"\n[EMAIL] Fetching emails for {total} rows (workers={max_workers}, requests-based)...")
    print(f"  Strategy: Footer → Homepage → {len(CONTACT_PATHS)} contact paths | Timeout: {WEBSITE_TIMEOUT}s")

    updated = 0
    skipped = 0
    checked = 0
    emails_since_save = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(lookup_email_requests, row["website_url"]): i
            for i, row in enumerate(candidates)
        }

        for future in as_completed(futures):
            i = futures[future]
            checked += 1

            try:
                email = future.result(timeout=WEBSITE_TIMEOUT + 5)
            except FutureTimeoutError:
                skipped += 1
                continue

            if email:
                candidates[i]["email"] = email
                updated += 1
                emails_since_save += 1
                print(f"  [{updated}] {candidates[i]['company_name'][:45]} → {email}")

                if emails_since_save >= AUTOSAVE_EVERY_NEW_EMAILS:
                    store.write_all(rows)
                    print(f"  [SAVE] {updated} emails → checkpoint")
                    emails_since_save = 0

            if checked % 50 == 0:
                print(f"  Progress: {checked}/{total}, found {updated}, skipped {skipped}")

    store.write_all(rows)
    print(f"\n[EMAIL] Done. {updated}/{total} rows got email. ({skipped} timed out)")
    return rows


# ===== MAIN SCRAPE FUNCTION =====

def scrape_kompass() -> tuple[pd.DataFrame, list[SupplierRecord]]:
    _log_kompass_proxy_mode_once()
    fetcher = initialize_fetcher()
    all_records: list[SupplierRecord] = []
    seen_company_names: set[str] = set()
    sink = StreamingCsvSink(
        output_paths=[PARTIAL_OUTPUT_CSV, OUTPUT_CSV],
        fieldnames=FIELDNAMES,
    )
    holder: Optional[KompassBrowserHolder] = None
    browser_page: Optional[Page] = None

    if ENABLE_BROWSER_FALLBACK and sync_playwright is not None:
        holder = KompassBrowserHolder()
        try:
            holder.playwright = sync_playwright().start()
            holder.endpoint = PROXY_POOL.get() if PROXY_POOL else None
            launch_kwargs: dict = {
                "headless": PLAYWRIGHT_HEADLESS,
                "args": PLAYWRIGHT_ARGS,
            }
            if PLAYWRIGHT_CHANNEL:
                launch_kwargs["channel"] = PLAYWRIGHT_CHANNEL
            if KOMPASS_USE_PROXY and KOMPASS_USE_WEBSHARE:
                launch_kwargs["proxy"] = _kompass_webshare_playwright_proxy()
                print(
                    f"[PROXY][WEBSHARE][KOMPASS] Playwright initial launch using "
                    f"{KOMPASS_WEBSHARE_HOST}:{KOMPASS_WEBSHARE_PORT}"
                )
            elif PROXY_POOL and holder.endpoint:
                cfg = PROXY_POOL.playwright_config(holder.endpoint)
                if cfg:
                    launch_kwargs["proxy"] = cfg
            try:
                holder.browser = holder.playwright.chromium.launch(**launch_kwargs)
            except Exception as exc:
                if launch_kwargs.pop("channel", None):
                    print(f"[BROWSER][WARN] Could not launch channel '{PLAYWRIGHT_CHANNEL}': {exc}")
                    holder.browser = holder.playwright.chromium.launch(**launch_kwargs)
                else:
                    raise
            browser_name = PLAYWRIGHT_CHANNEL if PLAYWRIGHT_CHANNEL else "chromium"
            print(f"[BROWSER][KOMPASS] initial browser={browser_name}")
            holder.context = holder.browser.new_context(
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                user_agent=DEFAULT_HEADERS["User-Agent"],
            )
            holder.page = holder.context.new_page()
            browser_page = holder.page
            try:
                if PROXY_POOL:
                    try:
                        new_p, new_e = goto_with_rotation(
                            holder.page,
                            BASE_DOMAIN,
                            PROXY_POOL,
                            holder.relaunch,
                            current_endpoint=holder.endpoint,
                            timeout_ms=60000,
                            wait_until="domcontentloaded",
                            validate=lambda h: "kompass" in h.lower() and len(h) > 800,
                        )
                        holder.page, holder.endpoint = new_p, new_e
                        browser_page = holder.page
                    except ProxyExhaustedError as exc:
                        print(f"[PROXY][WARN] {exc}")
                else:
                    holder.page.goto(BASE_DOMAIN, wait_until="domcontentloaded", timeout=60000)
                try:
                    accept_btn = holder.page.locator("button#axeptio_btn_acceptAll").first
                    if accept_btn.count() > 0 and accept_btn.is_visible(timeout=1200):
                        accept_btn.click(timeout=2500)
                        holder.page.wait_for_timeout(500)
                except Exception:
                    pass
                try:
                    close_btn = holder.page.locator("button.close[data-dismiss='modal']").first
                    if close_btn.count() > 0 and close_btn.is_visible(timeout=1200):
                        close_btn.click(timeout=2500)
                        holder.page.wait_for_timeout(400)
                except Exception:
                    pass
            except Exception:
                pass
        except Exception as exc:
            print(f"[BROWSER][WARN] Browser fallback disabled due to launch failure: {exc}")
            holder = None
            browser_page = None

    try:
        for keyword in KEYWORDS:
            seed_kompass_search_context(fetcher, keyword, holder)
            for country in COUNTRIES:
                seed_kompass_search_context(fetcher, keyword, holder)
                consecutive_zero_new_pages = 0
                for page in range(1, MAX_PAGES_PER_QUERY + 1):
                    html = ""
                    status_code = 0

                    live_page = holder.page if holder else browser_page
                    if page > 1 and live_page is not None:
                        status_code, html = click_kompass_pagination_page(live_page, page)

                    anchors: list[dict[str, str]] = []

                    if status_code == 200 and html:
                        anchors = parse_company_anchors_for_country(html, country)

                    if not anchors:
                        for url in build_search_urls(keyword, country, page):
                            try:
                                status_code, html = fetch_html_with_browser_fallback(
                                    fetcher, url, browser_holder=holder
                                )
                            except Exception:
                                continue
                            if status_code == 200 and html:
                                anchors = parse_company_anchors_for_country(html, country)
                                if anchors:
                                    break

                    if not anchors and page == 1:
                        seed_kompass_search_context(fetcher, keyword, holder)
                        for url in build_search_urls(keyword, country, page):
                            try:
                                status_code, html = fetch_html_with_browser_fallback(
                                    fetcher, url, browser_holder=holder
                                )
                            except Exception:
                                continue
                            if status_code == 200 and html:
                                anchors = parse_company_anchors_for_country(html, country)
                                if anchors:
                                    break

                    if status_code != 200 or not html:
                        print(
                            f"[WARN] Failed page load: keyword={keyword}, country={country}, page={page}"
                        )
                        random_delay()
                        continue
                    if not anchors:
                        print(
                            f"[INFO] Scraped page {page} (Kompass: {keyword} - {country}) - 0 suppliers."
                        )
                        random_delay()
                        break

                    new_count = 0
                    for anchor in anchors:
                        record = extract_company_record(anchor, keyword, country)
                        normalized_name = re.sub(r"\s+", " ", record.company_name).strip().lower()
                        if not normalized_name or normalized_name in seen_company_names:
                            continue
                        seen_company_names.add(normalized_name)
                        if PROFILE_ENRICH_DURING_SCRAPE:
                            enrich_from_profile(fetcher, record, browser_holder=holder)
                        all_records.append(record)
                        sink.append(record)
                        new_count += 1

                    print(
                        f"Scraped page {page} (Kompass: {keyword} - {country}) - {new_count} new suppliers"
                    )
                    print(f"Running total unique suppliers: {len(all_records)}")

                    if new_count == 0:
                        consecutive_zero_new_pages += 1
                    else:
                        consecutive_zero_new_pages = 0

                    if consecutive_zero_new_pages >= ZERO_NEW_PAGES_CUTOFF:
                        print(
                            f"[INFO] Hit {ZERO_NEW_PAGES_CUTOFF} consecutive 0-new pages "
                            f"for ({keyword} - {country}). Moving to next query."
                        )
                        break

                    random_delay()

                    if len(all_records) >= TARGET_SUPPLIERS:
                        print(f"[INFO] Target reached: {TARGET_SUPPLIERS} supplier records.")
                        break
                if len(all_records) >= TARGET_SUPPLIERS:
                    break
            if len(all_records) >= TARGET_SUPPLIERS:
                print(f"[INFO] Stopping scrape loop at {TARGET_SUPPLIERS} records.")
                break
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Ctrl+C received. Saving partial progress...")
        save_checkpoint(all_records, PARTIAL_OUTPUT_CSV)
        print(f"[PARTIAL-SAVED] {len(all_records)} records saved to {PARTIAL_OUTPUT_CSV}")
        return to_deduped_dataframe(all_records), all_records
    finally:
        if holder:
            if holder.context:
                try:
                    holder.context.close()
                except Exception:
                    pass
            if holder.browser:
                try:
                    holder.browser.close()
                except Exception:
                    pass
            if holder.playwright:
                try:
                    holder.playwright.stop()
                except Exception:
                    pass

    final_df = to_deduped_dataframe(all_records)
    if not final_df.empty:
        records = [SupplierRecord(**row) for row in final_df.to_dict("records")]
        sink.rewrite_full(records)
        print(
            f"[SAVE] Streamed and finalized {len(final_df)} rows to {OUTPUT_CSV} "
            f"and {PARTIAL_OUTPUT_CSV}"
        )
    return final_df, all_records


# ===== SEARCH FUNCTIONS =====

def click_kompass_pagination_page(browser_page: Optional[Page], page_number: int) -> tuple[int, str]:
    if browser_page is None or page_number <= 1:
        return 0, ""

    selectors = (
        f"#pagination-div-id a[href*='pageNbre={page_number}']",
        f"a[href*='/searchCompanies/scroll?tab=cmp'][href*='pageNbre={page_number}']",
    )
    for selector in selectors:
        try:
            link = browser_page.locator(selector).first
            if link.count() == 0 or not link.is_visible(timeout=1200):
                continue
            link.click(timeout=15000)
            try:
                browser_page.wait_for_load_state("domcontentloaded", timeout=30000)
            except Exception:
                pass
            try:
                browser_page.wait_for_load_state("networkidle", timeout=12000)
            except Exception:
                pass
            browser_page.wait_for_timeout(1200)
            html = browser_page.content()
            if html:
                return 200, html
        except Exception:
            continue
    return 0, ""


def build_warmup_url(keyword: str) -> str:
    return (
        f"{BASE_DOMAIN}/searchCompanies"
        f"?text={quote_plus(keyword)}&searchType=COMPANYNAME&page=1"
    )


def build_search_urls(keyword: str, country: str, page: int) -> list[str]:
    country_code = COUNTRY_CODES.get(country, "CN")
    query = quote_plus(keyword)
    country_label = quote_plus(country)
    return [
        (
            f"{BASE_DOMAIN}/searchCompanies/facet"
            f"?value={country_code}&label={country_label}&filterType=country"
            f"&searchType=COMPANYNAME&checked=true&text={query}&page={page}"
        ),
        (
            f"{BASE_DOMAIN}/searchCompanies"
            f"?text={query}&searchType=COMPANYNAME&page={page}"
            f"&localizationCode={country_code}&localizationType=COUNTRY"
            f"&localizationLabel={country_label}"
        ),
    ]


def seed_kompass_search_context(
    fetcher: Any,
    keyword: str,
    browser_holder: Optional[KompassBrowserHolder] = None,
) -> None:
    if browser_holder is None or browser_holder.page is None:
        return
    try:
        fetch_html_with_browser_fallback(
            fetcher, build_warmup_url(keyword), browser_holder=browser_holder
        )
    except Exception:
        pass


# ===== PARSE FUNCTIONS =====

def extract_anchor_hrefs_and_text(html: str) -> list[tuple[str, str]]:
    anchors: list[tuple[str, str]] = []
    for href, inner in re.findall(
        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        anchors.append((normalize_url(href.strip()), strip_tags(inner)))
    return anchors


def is_profile_url_for_country(profile_url: str, country: str) -> bool:
    expected_code = (COUNTRY_CODES.get(country, "") or "").lower()
    if not expected_code:
        return True
    path = (urlparse(profile_url).path or "").rstrip("/")
    profile_id = path.split("/")[-1].lower() if path else ""
    if not profile_id:
        return False
    return profile_id.startswith(expected_code)


def parse_company_anchors_for_country(html: str, country: str) -> list[dict[str, str]]:
    expected_code = (COUNTRY_CODES.get(country, "") or "").lower()
    anchors: list[dict[str, str]] = []
    seen_links: set[str] = set()

    for href, name in re.findall(
        r'<a[^>]+href=["\'](/c/[^"\']+)["\'][^>]*title=["\']([^"\']+)["\']',
        html,
        flags=re.I | re.S,
    ):
        clean_href = normalize_url(href)
        clean_name = strip_tags(name)
        if not clean_href or not clean_name:
            continue
        href_lower = clean_href.lower()
        if "/c/p/" in href_lower:
            continue
        if expected_code and not is_profile_url_for_country(clean_href, country):
            continue
        canon = href_lower.rstrip("/")
        if canon in seen_links:
            continue
        seen_links.add(canon)
        anchors.append({"href": clean_href, "name": clean_name})

    return anchors


def extract_company_record(anchor: dict[str, str], keyword: str, country: str) -> SupplierRecord:
    return SupplierRecord(
        company_name=anchor.get("name", "").strip() or "Unknown Supplier",
        website_url="",
        country=country or "Unknown",
        email="",
        profile_url=anchor.get("href", "").strip(),
    )


def enrich_from_profile(
    fetcher: Any,
    record: SupplierRecord,
    browser_holder: Optional[KompassBrowserHolder] = None,
) -> None:
    profile_url = (record.profile_url or "").strip()
    if not profile_url:
        return
    try:
        _, profile_html = fetch_html_with_browser_fallback(
            fetcher, profile_url, browser_holder=browser_holder
        )
    except Exception:
        return
    if not record.website_url:
        website = extract_external_website_from_profile(profile_html)
        if website:
            record.website_url = website


# ===== DATAFRAME HELPERS =====

def to_deduped_dataframe(records: list[SupplierRecord]) -> pd.DataFrame:
    df = pd.DataFrame([asdict(r) for r in records])
    if df.empty:
        return df
    df["company_name_norm"] = (
        df["company_name"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
    )
    return df.drop_duplicates(subset=["company_name_norm"]).drop(columns=["company_name_norm"])


def save_checkpoint(records: list[SupplierRecord], output_path: str) -> None:
    df = to_deduped_dataframe(records)
    if df.empty:
        return
    df.to_csv(output_path, index=False, encoding="utf-8-sig")


# ===== ENRICHMENT ORCHESTRATION =====

def run_enrichment(rows: list[dict], checkpoint_path: str, enriched_path: str) -> list[dict]:
    """Run Phase 2A (profile→website) and Phase 2B (website→email) enrichment."""

    checkpoint_store = CsvStore(checkpoint_path)
    checkpoint_rows = checkpoint_store.read_all()
    if checkpoint_rows:
        cp_index = {
            r.get("profile_url", "").strip(): r
            for r in checkpoint_rows
            if r.get("profile_url", "").strip()
        }
        merged = 0
        for row in rows:
            key = row.get("profile_url", "").strip()
            if key and key in cp_index:
                cp = cp_index[key]
                if not row.get("website_url") and cp.get("website_url"):
                    row["website_url"] = cp["website_url"]
                    merged += 1
                if not row.get("email") and cp.get("email"):
                    row["email"] = cp["email"]
        if merged:
            print(f"[CHECKPOINT] Merged {merged} values from previous run.")

    store = CsvStore(checkpoint_path)

    try:
        if ENABLE_PROFILE_WEBSITE_ENRICHMENT:
            print("[PROFILE] Starting profile-website enrichment pass...")
            rows = enrich_website_from_profile_rows(rows, store, MAX_PROFILE_WEBSITE_LOOKUPS)
            print("[PROFILE] Profile-website enrichment complete.")
        else:
            print("[PROFILE] Profile-website enrichment disabled.")

        if ENABLE_WEBSITE_EMAIL_ENRICHMENT:
            print("[WEBSITE] Starting website-email enrichment pass (requests-based, footer-first)...")
            rows = enrich_email_from_website_rows(
                rows, store, MAX_WEBSITE_EMAIL_LOOKUPS, max_workers=WEBSITE_EMAIL_WORKERS
            )
            print("[WEBSITE] Website-email enrichment complete.")
        else:
            print("[WEBSITE] Website-email enrichment disabled.")

    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Saving enrichment checkpoint...")
        store.write_all(rows)
        print(f"[SAVED] Checkpoint written to {checkpoint_path}")
        return rows

    enriched_store = CsvStore(enriched_path)
    enriched_store.write_all(rows)
    print(f"[SAVE] Enriched data saved to {enriched_path}")

    return rows


# ===== MAIN =====

def main() -> None:
    global WEBSITE_EMAIL_WORKERS, MAX_PROFILE_WEBSITE_LOOKUPS, MAX_WEBSITE_EMAIL_LOOKUPS, USE_PLAYWRIGHT_PROFILE

    parser = argparse.ArgumentParser(
        description="Kompass supplier scraper with integrated enrichment."
    )
    parser.add_argument("--skip-scrape", action="store_true",
                        help="Skip Phase 1 scraping; only run enrichment on existing CSV")
    parser.add_argument("--skip-enrichment", action="store_true",
                        help="Skip Phase 2 enrichment; only scrape")
    parser.add_argument("--input", default=OUTPUT_CSV,
                        help="Input CSV for enrichment (when skipping scrape)")
    parser.add_argument("--output", default=CLEANED_CSV,
                        help="Cleaned output CSV (only email-enriched records)")
    parser.add_argument("--enriched", default=ENRICHED_CSV,
                        help="Full enriched output CSV path")
    parser.add_argument("--checkpoint", default=CHECKPOINT_CSV,
                        help="Enrichment checkpoint CSV path")
    parser.add_argument("--workers", type=int, default=WEBSITE_EMAIL_WORKERS,
                        help="Threads for email step")
    parser.add_argument("--max-profile-lookups", type=int, default=MAX_PROFILE_WEBSITE_LOOKUPS,
                        help="Max profile pages to fetch (0=all)")
    parser.add_argument("--max-email-lookups", type=int, default=MAX_WEBSITE_EMAIL_LOOKUPS,
                        help="Max website pages to check for email (0=all)")
    parser.add_argument("--no-playwright-profile", action="store_true",
                        help="Use HTTP only for Kompass profile pages")
    args = parser.parse_args()

    WEBSITE_EMAIL_WORKERS = args.workers
    MAX_PROFILE_WEBSITE_LOOKUPS = args.max_profile_lookups
    MAX_WEBSITE_EMAIL_LOOKUPS = args.max_email_lookups
    if args.no_playwright_profile:
        USE_PLAYWRIGHT_PROFILE = False

    print("=" * 60)
    print("  Kompass Supplier Scraper + Enrichment")
    print(f"  Proxy    : {'ON' if KOMPASS_USE_PROXY else 'OFF'}")
    print(f"  Profile  : {'Playwright' if USE_PLAYWRIGHT_PROFILE else 'HTTP only'}")
    print(f"  Email    : requests-based (footer-first, {WEBSITE_TIMEOUT}s timeout)")
    print(f"  Workers  : {WEBSITE_EMAIL_WORKERS}")
    print(f"  Contact paths: {len(CONTACT_PATHS)}")
    print(f"  Cleaned output: {args.output}")
    print("=" * 60)

    all_rows: list[dict] = []

    if not args.skip_scrape:
        print("\n" + "=" * 60)
        print("  PHASE 1: SCRAPING")
        print("=" * 60)
        df, records = scrape_kompass()
        if df.empty:
            print("[INFO] No records found during scrape.")
        else:
            all_rows = df.to_dict("records")
            print(f"[PHASE 1] Scraped {len(all_rows)} unique records.")
    else:
        print("\n[SKIP] Phase 1 scraping skipped.")
        input_path = args.input
        if not os.path.exists(input_path):
            print(f"[ERROR] Input file not found: {input_path}")
            return
        with open(input_path, newline="", encoding="utf-8-sig") as fh:
            all_rows = list(csv.DictReader(fh))
        for row in all_rows:
            for f in FIELDNAMES:
                row.setdefault(f, "")
        print(f"[LOAD] {len(all_rows)} rows loaded from {input_path}")

    if not all_rows:
        print("[INFO] No rows to process. Exiting.")
        return

    if not args.skip_enrichment:
        print("\n" + "=" * 60)
        print("  PHASE 2: ENRICHMENT")
        print("=" * 60)
        all_rows = run_enrichment(all_rows, args.checkpoint, args.enriched)
    else:
        print("\n[SKIP] Phase 2 enrichment skipped.")

    cleaned_rows = [
        r for r in all_rows
        if (r.get("email") or "").strip() and is_useful_email(r["email"].strip())
    ]

    if cleaned_rows:
        cleaned_store = CsvStore(args.output)
        cleaned_store.write_all(cleaned_rows)
        print(f"\n[CLEANED] Saved {len(cleaned_rows)} email-enriched records to {args.output}")
    else:
        print("\n[CLEANED] No records with valid emails found.")

    with_website = sum(1 for r in all_rows if (r.get("website_url") or "").strip())
    with_email = sum(1 for r in all_rows if (r.get("email") or "").strip())
    print(f"\n{'=' * 60}")
    print("  FINAL STATS")
    print(f"  Total records    : {len(all_rows)}")
    print(f"  With website     : {with_website} ({with_website * 100 // max(1, len(all_rows))}%)")
    print(f"  With email       : {with_email} ({with_email * 100 // max(1, len(all_rows))}%)")
    print(f"  Cleaned (valid)  : {len(cleaned_rows)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
