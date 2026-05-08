"""
Kompass supplier scraper with integrated enrichment.
====================================================
Phase 1: Collect supplier records for cosmetic packaging terms
Phase 2: Enrich profile → website → email (footer-first with Playwright)
Output: Cleaned CSV with only email-enriched records

Features:
- Deduplication by company name
- Playwright browser fallback for anti-bot pages
- Proxy support (Webshare)
- Incremental checkpoint saves
- Concurrent email enrichment from company websites
- Footer-first email extraction using Playwright
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from html import unescape
from threading import Lock
from typing import Any, Generator, Optional
from urllib.parse import quote_plus, urlparse
from urllib.request import ProxyHandler, Request, build_opener

import pandas as pd
from proxy_service import (
    ProxyEndpoint,
    ProxyExhaustedError,
    create_proxy_pool,
    fetch_with_proxy_rotation,
    goto_with_rotation,
    script_proxy_enabled,
)

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
    # "cosmetic tubes",
    # "cosmetic jars",
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
TARGET_SUPPLIERS = 1500
AUTOSAVE_EVERY_NEW_RECORDS = 10
PROFILE_ENRICH_DURING_SCRAPE = False
ENABLE_PROFILE_WEBSITE_ENRICHMENT = True
MAX_PROFILE_WEBSITE_LOOKUPS = TARGET_SUPPLIERS
ENABLE_WEBSITE_EMAIL_ENRICHMENT = True
MAX_WEBSITE_EMAIL_LOOKUPS = TARGET_SUPPLIERS
WEBSITE_EMAIL_PATHS = ("", "/contact",
    "/contact-us", 
    "/contactus",
    "/contact.html",
    "/contact-us.html",
    "/contactus.html",
    "/about",
    "/about-us",
    "/about.html",
    "/about-us.html",
    "/contact/",
    "/contact-us/",
    "/contactus/",
    "/about/",
    "/about-us/",
    "/contactinfo",
    "/contact-info",
    "/contact_info",
    "/get-in-touch",
    "/reach-us",
    "/en/contact",
    "/en/contact-us",)
WEBSITE_EMAIL_WORKERS = 10
# Email extraction config
WEBSITE_EMAIL_USE_PLAYWRIGHT_PAGE = True  # Use Playwright for footer-aware email extraction
WEBSITE_EMAIL_PLAYWRIGHT_TIMEOUT = 30000  # 30s timeout for individual page loads
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
ENABLE_BROWSER_FALLBACK = True
PLAYWRIGHT_HEADLESS = False
PLAYWRIGHT_CHANNEL = (
    os.environ["KOMPASS_PLAYWRIGHT_CHANNEL"].strip()
    if "KOMPASS_PLAYWRIGHT_CHANNEL" in os.environ
    else "chrome"
)
# Phase 2A: Use Playwright for profile pages
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

# Domains to skip when extracting external website (Kompass/KSales sales properties, social, etc.)
BLOCKED_DOMAINS = (
    "kompass.com", "ksales.ai", "facebook.com", "instagram.com",
    "linkedin.com", "youtube.com", "wa.me", "twitter.com",
    "tiktok.com", "pinterest.com",
)

# Emails to reject
BLOCKED_EMAIL_TOKENS = [
    "kompass.com", "alibaba.com", "made-in-china.com",
    "cloudflare", "404", "notfound", "blocked", "error",
    "copyright", "@anytime", "@theforefront", "@homeandabroad",
    "@thistime",
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
    """True if this URL must not be crawled for supplier email (Kompass/KSales, social, etc.)."""
    return not is_plausible_external_website((url or "").strip())


def clean_email(email: str) -> str:
    """Clean email by removing trailing text/junk after domain."""
    if not email:
        return ""
    match = re.match(r'([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})([A-Z].*)?', email)
    return match.group(1) if match else email


def is_useful_email(email: str) -> bool:
    """Filter junk and platform emails."""
    if not email or len(email) > 80 or len(email) < 6:
        return False
    if ' ' in email or email.count('@') != 1:
        return False
    if not re.search(r'\.(com|net|org|cn|kr|jp|tw|vn|th|sg|my|hk|co\.\w+)$', email, re.I):
        return False
    low = email.lower()
    if any(b in low for b in BLOCKED_EMAIL_TOKENS):
        return False
    return True


def extract_email_from_text_flexible(text: str) -> Optional[str]:
    if not text:
        return None
    text = unescape(text)
    mailto_match = re.search(
        r"mailto:([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})",
        text,
        re.I,
    )
    if mailto_match:
        return mailto_match.group(1)
    normalized = re.sub(r"<[^>]+>", " ", text)
    normalized = re.sub(r"\s*\(at\)\s*|\s*\[at\]\s*|\s+at\s+", "@", normalized, flags=re.I)
    normalized = re.sub(
        r"\s*\(dot\)\s*|\s*\[dot\]\s*|\s+dot\s+",
        ".",
        normalized,
        flags=re.I,
    )
    normalized = re.sub(r"\s+", "", normalized)
    match = re.search(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", normalized)
    return match.group(0) if match else None


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


def fetch_profile_html_playwright(page: Any, url: str) -> tuple[int, str]:
    """Load a Kompass profile in the shared Playwright page."""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_PROFILE_TIMEOUT_MS)
        page.wait_for_timeout(600)
        html = page.content()
        if not html:
            return 0, ""
        return 200, html
    except PlaywrightTimeoutError:
        print(f"    [PLAYWRIGHT TIMEOUT] {url[:80]}")
        return 0, ""
    except Exception as exc:
        print(f"    [PLAYWRIGHT ERROR] {url[:80]} → {exc}")
        return 0, ""


# ===== PHASE 2A: PROFILE → WEBSITE =====

def extract_external_website_from_profile(profile_html: str) -> str:
    """Extract external website URL from Kompass profile page HTML."""
    # 1. Dedicated website row
    row_match = re.search(
        r'<tr[^>]*class=["\'][^"\']*\btrWebSite\b[^"\']*["\'][^>]*>(.*?)</tr>',
        profile_html,
        flags=re.I | re.S,
    )
    if row_match:
        for href, _ in _extract_anchors(row_match.group(1)):
            if is_plausible_external_website(href):
                return href

    # 2. Look for a clearly labelled website section
    website_section = re.search(
        r'(?:website|web site|www)[^<]{0,60}<[^>]*href=["\']([^"\']+)["\']',
        profile_html,
        flags=re.I,
    )
    if website_section:
        href = normalize_url(website_section.group(1))
        if is_plausible_external_website(href):
            return href

    # 3. Fallback: first plausible external link
    for href, _ in _extract_anchors(profile_html):
        if is_plausible_external_website(href):
            return href

    return ""


def _extract_anchors(html: str) -> list[tuple[str, str]]:
    results = []
    for href, inner in re.findall(
        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        html,
        flags=re.I | re.S,
    ):
        results.append((normalize_url(href.strip()), strip_tags(inner)))
    return results


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
                status, html = fetch_profile_html_playwright(pw_page, profile_url)
            else:
                fetcher = initialize_fetcher()
                status, html = fetch_html(fetcher, profile_url)

            if status != 200 or not html:
                label = "browser" if pw_page is not None else "HTTP"
                print(f"✗ ({label} status {status})")
                random_delay(min_sec=2, max_sec=5)
                continue

            website = extract_external_website_from_profile(html)
            if website:
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


# ===== PHASE 2B: WEBSITE → EMAIL (FOOTER-FIRST WITH PLAYWRIGHT) =====

def _extract_email_from_page(page: Any) -> Optional[str]:
    """Extract email from a fully loaded Playwright page."""
    try:
        html = page.content()
        return extract_email_from_text_flexible(html)
    except Exception:
        return None


def _fetch_email_for_row(row: dict) -> tuple[dict, str]:
    """
    Worker: visit company website with Playwright.
    Priority:
    1. Homepage footer → scroll to bottom, extract email from footer area
    2. Contact pages (/, /contact, /contact-us, /about, /about-us)
    """
    if is_blocked_platform_website_url((row.get("website_url") or "").strip()):
        return row, ""

    if not WEBSITE_EMAIL_USE_PLAYWRIGHT_PAGE or sync_playwright is None:
        # Fallback to simple HTTP if Playwright not available
        base = row["website_url"].rstrip("/")
        seen: set[str] = set()
        for path in WEBSITE_EMAIL_PATHS:
            url = f"{base}{path}" if path else base
            if url in seen:
                continue
            seen.add(url)
            _, html = fetch_html_simple(url, timeout=20)
            if not html:
                continue
            email = extract_email_from_text_flexible(html)
            if email:
                email = clean_email(email)
                if is_useful_email(email):
                    return row, email
        return row, ""

    # Playwright-based footer-first extraction
    pw = None
    browser = None
    try:
        pw = sync_playwright().start()
        launch_kwargs: dict[str, Any] = {
            "headless": PLAYWRIGHT_HEADLESS,
            "args": PLAYWRIGHT_ARGS,
        }
        if PLAYWRIGHT_CHANNEL:
            launch_kwargs["channel"] = PLAYWRIGHT_CHANNEL
        if KOMPASS_USE_PROXY and KOMPASS_USE_WEBSHARE:
            launch_kwargs["proxy"] = _kompass_webshare_playwright_proxy()

        browser = pw.chromium.launch(**launch_kwargs)
        context = browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            user_agent=DEFAULT_HEADERS["User-Agent"],
        )
        page = context.new_page()
        base = row["website_url"].rstrip("/")
        seen_urls: set[str] = set()

        # ============================================================
        # STEP 1: Check homepage footer first
        # ============================================================
        try:
            page.goto(base, wait_until="domcontentloaded", timeout=WEBSITE_EMAIL_PLAYWRIGHT_TIMEOUT)
            page.wait_for_timeout(1500)

            # Scroll to the very bottom of the page to trigger lazy-loaded footers
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)

            # Try multiple footer-specific extraction strategies
            footer_html_parts: list[str] = []

            # Strategy A: Extract <footer> tag content
            try:
                footer_elements = page.locator("footer").all()
                for el in footer_elements:
                    try:
                        html = el.inner_html()
                        if html:
                            footer_html_parts.append(html)
                    except Exception:
                        pass
            except Exception:
                pass

            # Strategy B: Extract elements with footer-related class/ID names
            footer_selectors = [
                ".footer",
                "#footer",
                ".site-footer",
                "#site-footer",
                ".page-footer",
                "#page-footer",
                "[class*='footer']",
                "[id*='footer']",
                ".bottom",
                "#bottom",
                ".copyright",
                "#copyright",
            ]
            for selector in footer_selectors:
                try:
                    elements = page.locator(selector).all()
                    for el in elements:
                        try:
                            html = el.inner_html()
                            if html:
                                footer_html_parts.append(html)
                        except Exception:
                            pass
                except Exception:
                    pass

            # Strategy C: Get the last 20% of the page HTML (rough footer area)
            try:
                full_html = page.content()
                # Take the last 25% of the HTML as potential footer area
                split_point = int(len(full_html) * 0.75)
                bottom_html = full_html[split_point:]
                footer_html_parts.append(bottom_html)
            except Exception:
                pass

            # Search for email in all collected footer parts
            for footer_html in footer_html_parts:
                email = extract_email_from_text_flexible(footer_html)
                if email:
                    email = clean_email(email)
                    if is_useful_email(email):
                        return row, email

            # Strategy D: Also check full page for mailto: links in footer area
            try:
                mailto_links = page.locator("footer a[href*='mailto:']").all()
                if not mailto_links:
                    mailto_links = page.locator("[class*='footer'] a[href*='mailto:']").all()
                if not mailto_links:
                    mailto_links = page.locator("[id*='footer'] a[href*='mailto:']").all()
                for link in mailto_links:
                    try:
                        href = link.get_attribute("href")
                        if href and "mailto:" in href:
                            email = href.replace("mailto:", "").split("?")[0].strip()
                            email = clean_email(email)
                            if is_useful_email(email):
                                return row, email
                    except Exception:
                        continue
            except Exception:
                pass

        except PlaywrightTimeoutError:
            pass
        except Exception:
            pass

        # ============================================================
        # STEP 2: If no email in footer, check contact pages
        # ============================================================
        for path in WEBSITE_EMAIL_PATHS:
            url = f"{base}{path}" if path else base
            if url in seen_urls:
                continue
            seen_urls.add(url)

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=WEBSITE_EMAIL_PLAYWRIGHT_TIMEOUT)
                page.wait_for_timeout(1500)

                email = _extract_email_from_page(page)
                if email:
                    email = clean_email(email)
                    if is_useful_email(email):
                        return row, email
            except PlaywrightTimeoutError:
                continue
            except Exception:
                continue

        return row, ""

    except Exception as exc:
        print(f"    [PLAYWRIGHT WORKER ERROR] {row.get('website_url', '')[:60]} → {exc}")
        return row, ""
    finally:
        try:
            if browser:
                browser.close()
        except Exception:
            pass
        try:
            if pw:
                pw.stop()
        except Exception:
            pass


def enrich_email_from_website_rows(
    rows: list[dict],
    store: CsvStore,
    max_lookups: int = 0,
    max_workers: int = 4,
) -> list[dict]:
    """
    For every row with website_url but no email, visit the website and
    hunt for an email address (concurrently with Playwright).
    
    Args:
        max_workers: Limited to prevent excessive memory/browser instances.
                     Default 4 is safe for most machines.
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

    print(f"\n[EMAIL] Fetching emails for {total} rows (workers={max_workers}, footer-first with Playwright)...")

    updated = 0
    checked = 0
    lock = Lock()

    def _cb(future):
        nonlocal updated, checked
        row, email = future.result()
        with lock:
            checked += 1
            if email:
                row["email"] = email
                updated += 1
                print(f"  [{checked}/{total}] ✓ {row['company_name'][:45]:<45}  {email}")
            else:
                print(f"  [{checked}/{total}] – {row['company_name'][:45]:<45}  (no email)")

            if checked % AUTOSAVE_EVERY_NEW_RECORDS == 0:
                store.write_all(rows)
                print(f"  [CHECKPOINT] Saved after {checked} email lookups ({updated} found).")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_email_for_row, r): r for r in candidates}
        for f in as_completed(futures):
            _cb(f)

    store.write_all(rows)
    print(f"\n[EMAIL] Done. {updated}/{total} rows got email.")
    return rows


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


# ===== ENRICHMENT ORCHESTRATION =====

def run_enrichment(rows: list[dict], checkpoint_path: str, enriched_path: str) -> list[dict]:
    """Run Phase 2A (profile→website) and Phase 2B (website→email) enrichment."""
    
    # Merge any existing checkpoint data
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
        print(f"[CHECKPOINT] Merged {merged} website_url values from previous run.")

    store = CsvStore(checkpoint_path)

    try:
        # Phase 2A: Profile → Website
        if ENABLE_PROFILE_WEBSITE_ENRICHMENT:
            print("[PROFILE] Starting profile-website enrichment pass...")
            rows = enrich_website_from_profile_rows(rows, store, MAX_PROFILE_WEBSITE_LOOKUPS)
            print("[PROFILE] Profile-website enrichment complete.")
        else:
            print("[PROFILE] Profile-website enrichment disabled.")

        # Phase 2B: Website → Email
        if ENABLE_WEBSITE_EMAIL_ENRICHMENT:
            print("[WEBSITE] Starting website-email enrichment pass (footer-first with Playwright)...")
            # Limit workers to prevent excessive browser instances
            email_workers = min(WEBSITE_EMAIL_WORKERS, 4)
            rows = enrich_email_from_website_rows(
                rows, store, MAX_WEBSITE_EMAIL_LOOKUPS, max_workers=email_workers
            )
            print("[WEBSITE] Website-email enrichment complete.")
        else:
            print("[WEBSITE] Website-email enrichment disabled.")

    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Saving enrichment checkpoint...")
        store.write_all(rows)
        print(f"[SAVED] Checkpoint written to {checkpoint_path}")
        return rows

    # Save enriched output
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
    parser.add_argument("--no-playwright-email", action="store_true",
                        help="Use simple HTTP (not Playwright) for email extraction")
    args = parser.parse_args()

    WEBSITE_EMAIL_WORKERS = args.workers
    MAX_PROFILE_WEBSITE_LOOKUPS = args.max_profile_lookups
    MAX_WEBSITE_EMAIL_LOOKUPS = args.max_email_lookups
    if args.no_playwright_profile:
        USE_PLAYWRIGHT_PROFILE = False
    if args.no_playwright_email:
        global WEBSITE_EMAIL_USE_PLAYWRIGHT_PAGE
        WEBSITE_EMAIL_USE_PLAYWRIGHT_PAGE = False

    print("=" * 60)
    print("  Kompass Supplier Scraper + Enrichment")
    print(f"  Proxy    : {'ON' if KOMPASS_USE_PROXY else 'OFF'}")
    print(f"  Profile  : {'Playwright' if USE_PLAYWRIGHT_PROFILE else 'HTTP only'}")
    print(f"  Email    : {'Playwright (footer-first)' if WEBSITE_EMAIL_USE_PLAYWRIGHT_PAGE else 'HTTP simple'}")
    print(f"  Workers  : {WEBSITE_EMAIL_WORKERS}")
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
        # Ensure all fieldnames exist
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

    # Generate cleaned output: only rows with valid email
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

    # Final stats
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