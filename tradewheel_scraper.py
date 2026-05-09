"""
Tradewheel supplier scraper - Optimized with login enrichment, email sanitization, and cleaned CSV.
Phase 3 upgraded with requests-based email enrichment (same as EC21/Made-in-China).
"""

from __future__ import annotations

import random
import re
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from dataclasses import dataclass, asdict
from html import unescape
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode, urlparse

import pandas as pd
import requests
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
    "cosmetic packaging",
    "cosmetic bottles",
    "cosmetic tubes",
    "cosmetic jars",
    "plastic packaging",
    "glass packaging",
    "lotion pumps",
    "airless pumps",
    "cosmetics"
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

SOURCE_DIRECTORY = "Tradewheel"
BASE_DOMAIN = "https://www.tradewheel.com"
MAX_PAGES_PER_QUERY = 3
MIN_DELAY_SECONDS = 3
MAX_DELAY_SECONDS = 7
OUTPUT_CSV = "tradewheel_suppliers_raw.csv"
CLEANED_CSV = "tradewheel_suppliers_cleaned.csv"
PARTIAL_OUTPUT_CSV = "tradewheel_suppliers_partial.csv"
TARGET_SUPPLIERS = 5000
AUTOSAVE_EVERY_NEW_RECORDS = 10

ENABLE_LOGGED_IN_ENRICHMENT = True
PAUSE_FOR_MANUAL_LOGIN = True
AUTH_BROWSER_PROFILE_DIR = ".tradewheel_auth_profile"
AUTH_BROWSER_HEADLESS = False
MAX_LOGGED_IN_ENRICHMENTS = 800
PLAYWRIGHT_AUTH_CHANNEL = "msedge"
PLAYWRIGHT_AUTH_EXTRA_ARGS = ["--disable-blink-features=AutomationControlled"]
TRADEWHEEL_EMAIL_ENV = "TRADEWHEEL_EMAIL"
TRADEWHEEL_PASSWORD_ENV = "TRADEWHEEL_PASSWORD"

# SEARCH PAGES (Cloudflare-sensitive)
USE_BROWSER_FOR_SEARCH = True
SEARCH_BROWSER_PROFILE_DIR = ".tradewheel_search_profile"
SEARCH_BROWSER_HEADLESS = False
SEARCH_BROWSER_TIMEOUT_SECONDS = 45
PLAYWRIGHT_SEARCH_CHANNEL = "msedge"
PLAYWRIGHT_SEARCH_EXTRA_ARGS = ["--disable-blink-features=AutomationControlled"]

# WEBSITE EMAIL ENRICHMENT (UPGRADED - requests-based, same as EC21/MIC)
ENABLE_WEBSITE_EMAIL_ENRICHMENT = True
MAX_WEBSITE_EMAIL_LOOKUPS = TARGET_SUPPLIERS
WEBSITE_EMAIL_WORKERS = 10
WEBSITE_TIMEOUT = 15
AUTOSAVE_EVERY_NEW_EMAILS = 10

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

# Tradewheel /search/company/?country= numeric IDs
COUNTRY_IDS = {
    "China": 43, "South Korea": 111, "Taiwan": 210, "Japan": 103,
    "Vietnam": 220, "Thailand": 201, "Singapore": 183,
    "Malaysia": 145, "Hong Kong": 88,
}

KEYWORDS = env_list("SCRAPER_KEYWORDS", KEYWORDS)
COUNTRIES = env_list("SCRAPER_COUNTRIES", COUNTRIES)
TARGET_SUPPLIERS = env_int("SCRAPER_TARGET_SUPPLIERS", TARGET_SUPPLIERS)

PROXY_POOL = create_proxy_pool("tradewheel")

_TRADEWHEEL_SCRAPLING_CONFIGURED = False


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


def _tw_has_supplier_listing_signals(html: str) -> bool:
    if not html:
        return False
    lo = html.lower()
    if "/co/" in lo or "company_info.html" in lo:
        return True
    if "tradewheel" in lo and re.search(
        r"product\(s\)|suppliers?\s+list|manufacturers?\s+and\s+suppliers", lo
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


def random_delay():
    time.sleep(random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))


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
    blocked = ("facebook.com", "instagram.com", "linkedin.com", "youtube.com", "wa.me")
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


def build_search_url(keyword: str, country: str, page: int) -> str:
    params: dict[str, str] = {"keyword": (keyword or "").strip()}
    cid = COUNTRY_IDS.get(country)
    if cid is not None:
        params["country"] = str(cid)
    if page > 1:
        params["page"] = str(page)
    return f"{BASE_DOMAIN}/search/company/?{urlencode(params)}"


def fetch_html(url: str) -> tuple[int, str]:
    _configure_scrapling_once()
    validator = _validator_for_url(url)

    if PROXY_POOL is None:
        return _stealthy_fetch_with_cloudflare(url)

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
            if sf_html and not _is_cf_challenge_html(sf_html):
                return sf_status, sf_html
        return st, text
    except ProxyExhaustedError as exc:
        print(f"[PROXY][WARN] {exc}")
        if _env_bool("TRADEWHEEL_STEALTHY_CF_FALLBACK", True):
            return _stealthy_fetch_with_cloudflare(url)
        return 0, ""
    except Exception as exc:
        if _env_bool("TRADEWHEEL_STEALTHY_CF_FALLBACK", True):
            print(f"[FETCH][TRADEWHEEL][WARN] {exc!r}; retrying with StealthyFetcher (solve_cloudflare)")
            return _stealthy_fetch_with_cloudflare(url)
        return 0, ""


def _looks_like_cloudflare(url: str, html: str) -> bool:
    u = (url or "").lower()
    if "cdn-cgi/challenge" in u or "challenges.cloudflare.com" in u:
        return True
    return _is_cf_challenge_html(html)


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

    def fetch(self, url: str) -> tuple[int, str]:
        if not self.enabled or self.page is None:
            return 0, ""
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
            self.page.wait_for_timeout(1200)
            _dismiss_tradewheel_signup_modal(self.page)
            html = self.page.content()
            if _tw_listing_validator(html):
                return 200, html
            return 200, html
        except Exception:
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
    candidates = [href for href, _ in extract_anchor_hrefs_and_text(profile_html) if is_plausible_website(href)]
    return candidates[0] if candidates else ""


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
        self.enabled = sync_playwright is not None and ENABLE_LOGGED_IN_ENRICHMENT
        self.auth_email = os.getenv(TRADEWHEEL_EMAIL_ENV, "").strip()
        self.auth_password = os.getenv(TRADEWHEEL_PASSWORD_ENV, "")

    def _relaunch_auth_persistent(self, ep: Optional[ProxyEndpoint]):
        if self.context:
            try: self.context.close()
            except: pass
        self.context = None
        self.page = None
        launch_kwargs = dict(
            user_data_dir=str(Path(AUTH_BROWSER_PROFILE_DIR).resolve()),
            headless=AUTH_BROWSER_HEADLESS,
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="Asia/Shanghai",
            args=PLAYWRIGHT_AUTH_EXTRA_ARGS,
        )
        if PLAYWRIGHT_AUTH_CHANNEL:
            launch_kwargs["channel"] = PLAYWRIGHT_AUTH_CHANNEL
        if PROXY_POOL:
            cfg = PROXY_POOL.playwright_config(ep) if ep is not None else PROXY_POOL.playwright_config()
            if cfg:
                launch_kwargs["proxy"] = cfg
        try:
            self.context = self.playwright.chromium.launch_persistent_context(**launch_kwargs)
        except Exception as exc:
            if launch_kwargs.pop("channel", None):
                print(f"[AUTH][WARN] Could not launch channel '{PLAYWRIGHT_AUTH_CHANNEL}': {exc}")
                self.context = self.playwright.chromium.launch_persistent_context(**launch_kwargs)
            else:
                raise
        self.page = self.context.new_page()
        self._proxy_endpoint = ep
        return self.page

    def _auth_goto(self, url: str, timeout_ms: int = 120000) -> None:
        if self.page is None:
            return
        if not PROXY_POOL:
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

    def __enter__(self):
        if not self.enabled:
            return self
        self.playwright = sync_playwright().start()
        self._proxy_endpoint = PROXY_POOL.get() if PROXY_POOL else None
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
        return self.enabled and self.page is not None and self.attempts < self.max_attempts

    def enrich(self, profile_url: str) -> tuple[str, str]:
        if not self.can_run() or not profile_url:
            return "", ""
        self.attempts += 1
        try:
            self._auth_goto(profile_url, 90000)
            self.page.wait_for_timeout(1200)
            _dismiss_tradewheel_signup_modal(self.page)
            for xpath in (
                "//tr[td[contains(translate(normalize-space(.), 'EMAIL', 'email'), 'email')]]//a[contains(., 'Show')]",
                "//tr[td[contains(translate(normalize-space(.), 'WEBSITE', 'website'), 'website')]]//a[contains(., 'Show')]",
            ):
                try:
                    link = self.page.locator(f"xpath={xpath}").first
                    if link.count() > 0:
                        link.click(timeout=2500)
                        self.page.wait_for_timeout(1200)
                except: continue
        except: return "", ""
        html = ""
        for _ in range(3):
            try:
                self.page.wait_for_load_state("domcontentloaded", timeout=7000)
                html = self.page.content()
                if html: break
            except: self.page.wait_for_timeout(800)
        if not html: return "", ""
        website_url = extract_external_website_from_profile(html)
        email = extract_email_from_profile_table(html)
        return website_url, email


def run_logged_in_enrichment(records: list[SupplierRecord], max_records: Optional[int] = None):
    if not ENABLE_LOGGED_IN_ENRICHMENT: return
    with LoggedInContactEnricher() as auth_enricher:
        if not auth_enricher.enabled: return
        processed = 0
        updated = 0
        for record in records:
            if max_records and processed >= max_records: break
            if record.website_url and record.email: continue
            if not auth_enricher.can_run(): break
            website_url, email = auth_enricher.enrich(record.profile_url)
            changed = False
            if website_url and not record.website_url:
                record.website_url = website_url
                changed = True
            if email and not record.email and is_useful_email(email):
                record.email = email
                changed = True
            processed += 1
            if changed: updated += 1
            if processed % 25 == 0:
                print(f"[AUTH] Processed {processed} profiles; updated {updated}.")


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


def enrich_emails_from_company_websites(records: list[SupplierRecord]):
    """Visit company websites to find emails (requests-based, same as EC21/MIC)."""
    if not ENABLE_WEBSITE_EMAIL_ENRICHMENT:
        return
    
    candidates = [r for r in records if r.website_url and not r.email][:MAX_WEBSITE_EMAIL_LOOKUPS]
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
                    save_checkpoint(records, PARTIAL_OUTPUT_CSV)
                    print(f"  [SAVE] {found} emails → {PARTIAL_OUTPUT_CSV}")
                    emails_since_save = 0
            
            if i % 50 == 0:
                print(f"  Progress: {i}/{total}, found {found}, skipped {skipped}")
    
    save_checkpoint(records, PARTIAL_OUTPUT_CSV)
    print(f"  Total emails found: {found} ({skipped} timed out)")


# ===== SAVE FUNCTIONS =====

def save_checkpoint(records: list[SupplierRecord], path: str):
    if not records:
        return
    df = pd.DataFrame([asdict(r) for r in records])
    df["name_norm"] = df["company_name"].str.lower().str.strip()
    df = df.drop_duplicates(subset=["name_norm"]).drop(columns=["name_norm"])
    df.to_csv(path, index=False, encoding="utf-8-sig", sep='\t')


# ===== MAIN =====

def scrape_tradewheel():
    all_records = []
    seen_names = set()
    save_counter = 0

    print("=" * 60)
    print("Tradewheel Cosmetic Packaging Supplier Scraper")
    print(f"Target: {TARGET_SUPPLIERS} suppliers")
    print(f"Email: requests-based, {len(CONTACT_PATHS)} contact paths, {WEBSITE_TIMEOUT}s timeout")
    print("=" * 60)

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
                    for page in range(1, MAX_PAGES_PER_QUERY + 1):
                        if len(all_records) >= TARGET_SUPPLIERS: break

                        url = build_search_url(keyword, country, page)
                        try:
                            if search_browser.enabled:
                                status, html = search_browser.fetch(url)
                            else:
                                status, html = fetch_html(url)
                        except:
                            random_delay()
                            continue

                        if status == 404 or not html: break
                        anchors = parse_company_anchors(html)
                        if not anchors: break

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

                        print(f"  [{keyword}] [{country}] Page {page}: +{page_new} (Total: {len(all_records)})")
                        random_delay()

    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Saving progress...")
        save_checkpoint(all_records, PARTIAL_OUTPUT_CSV)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        save_checkpoint(all_records, PARTIAL_OUTPUT_CSV)

    # Phase 2: Logged-in enrichment (Playwright - unchanged)
    if all_records and ENABLE_LOGGED_IN_ENRICHMENT:
        print("\n[AUTH] Starting logged-in enrichment...")
        try:
            run_logged_in_enrichment(all_records)
        except Exception as e:
            print(f"[AUTH] Error: {e}")
        save_checkpoint(all_records, PARTIAL_OUTPUT_CSV)

    # Phase 3: Website email enrichment (NEW: requests-based)
    if all_records and ENABLE_WEBSITE_EMAIL_ENRICHMENT:
        print("\n[WEBSITE] Starting website email enrichment...")
        try:
            enrich_emails_from_company_websites(all_records)
        except Exception as e:
            print(f"[WEBSITE] Error: {e}")
        save_checkpoint(all_records, PARTIAL_OUTPUT_CSV)

    # Final output
    df = pd.DataFrame([asdict(r) for r in all_records])
    df["name_norm"] = df["company_name"].str.lower().str.strip()
    df = df.drop_duplicates(subset=["name_norm"]).drop(columns=["name_norm"])
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig", sep='\t')

    df_clean = df[df['email'].notna() & (df['email'] != '')]
    df_clean.to_csv(CLEANED_CSV, index=False, encoding="utf-8-sig", sep='\t')

    print(f"\n{'='*60}")
    print(f"DONE!")
    print(f"  Raw: {OUTPUT_CSV} ({len(df)} suppliers)")
    print(f"  With website: {(df['website_url'] != '').sum()}")
    print(f"  With email: {(df['email'] != '').sum()}")
    print(f"  Cleaned: {CLEANED_CSV} ({len(df_clean)} suppliers)")
    print(f"{'='*60}")

    return df


def main():
    df = scrape_tradewheel()
    if df.empty:
        print("[INFO] No records found.")


if __name__ == "__main__":
    main()