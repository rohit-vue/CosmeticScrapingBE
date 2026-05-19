"""
Made-in-China supplier scraper - Standalone with own keywords and countries.
Playwright for contact page enrichment, requests for email extraction.
"""

from __future__ import annotations

import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from dataclasses import asdict, dataclass
from html import unescape
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus, urlparse
from urllib.request import Request, urlopen
from urllib.parse import quote

import pandas as pd
import requests
# AI filtering is currently disabled so the scraper jumps straight to email enrichment.
# from ai_supplier_filter import apply_ai_filter_to_records
from scraper_runtime_config import env_int, env_list

try:
    from scrapling import StealthFetcher as ScraplingStealthFetcher
except ImportError:
    from scrapling import StealthyFetcher as ScraplingStealthFetcher

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None
    PlaywrightTimeoutError = Exception


# ===== KEYWORDS =====
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

# ===== CONFIGURATION =====
DEFAULT_COUNTRY = "China"
SOURCE_DIRECTORY = "Made-in-China"
BASE_DOMAIN = "https://www.made-in-china.com"
MAX_PAGES_PER_QUERY = 70
ZERO_NEW_PAGES_CUTOFF = 3
MIN_DELAY_SECONDS = 3
MAX_DELAY_SECONDS = 7
OUTPUT_CSV = "made_in_china_suppliers_phase1_raw.csv"
CLEANED_CSV = "made_in_china_suppliers_cleaned.csv"
PARTIAL_SCRAPE_CSV = "made_in_china_suppliers_partial_scrape.csv"
PARTIAL_ENRICH_CSV = "made_in_china_suppliers_partial_enrichment.csv"
TARGET_SUPPLIERS = 10000
AUTOSAVE_EVERY_NEW_RECORDS = 10
AUTOSAVE_EVERY_NEW_EMAILS = 1
PROFILE_ENRICH_DURING_SCRAPE = False
ENABLE_CONTACT_PAGE_ENRICHMENT = True
MAX_CONTACT_ENRICHMENTS = 0
ENABLE_WEBSITE_EMAIL_ENRICHMENT = True
MAX_WEBSITE_EMAIL_LOOKUPS = 0
WEBSITE_EMAIL_WORKERS = 10
WEBSITE_TIMEOUT = 15
ENABLE_AI_FILTERING = False
AI_VERIFIED_CSV = "made_in_china_ai_verified.csv"
AI_REJECTED_CSV = "made_in_china_ai_rejected.csv"
AI_CHECKPOINT_CSV = "made_in_china_ai_checkpoint.csv"
AI_BATCH_SIZE = int(os.getenv("AI_BATCH_SIZE", "20"))
AI_CONCURRENT = int(os.getenv("AI_CONCURRENT", "3"))

# Extended contact paths
CONTACT_PATHS = [
    "",
    "/contact",
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
    "/en/contact-us",
]

MIC_SITE_GAP_SECONDS = float(os.getenv("MIC_SITE_GAP_SECONDS", "1.5") or "1.5")
MIC_SITE_GAP_JITTER_SECONDS = float(os.getenv("MIC_SITE_GAP_JITTER_SECONDS", "0") or "0")

CONTACT_ENRICHMENT_USE_PLAYWRIGHT = True
MIC_CONTACT_PROFILE_DIR = ".mic_contact_playwright_profile"
CONTACT_ENRICHMENT_BROWSER_TIMEOUT_MS = 90000
PLAYWRIGHT_CONTACT_CHANNEL = (os.getenv("MIC_PLAYWRIGHT_CHANNEL") or "").strip()
PLAYWRIGHT_CONTACT_EXTRA_ARGS = ["--disable-blink-features=AutomationControlled"]

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

KEYWORDS = env_list("SCRAPER_KEYWORDS", KEYWORDS)
TARGET_SUPPLIERS = env_int("SCRAPER_TARGET_SUPPLIERS", TARGET_SUPPLIERS)
MAX_CONTACT_ENRICHMENTS = env_int("MADE_IN_CHINA_MAX_CONTACT_ENRICHMENTS", MAX_CONTACT_ENRICHMENTS)
MAX_WEBSITE_EMAIL_LOOKUPS = env_int("MADE_IN_CHINA_MAX_EMAIL_LOOKUPS", MAX_WEBSITE_EMAIL_LOOKUPS)

JUNK_EMAIL_PHRASES = [
    "cloudflare", "404", "notfound", "blocked", "error",
    "ordercreditreport", "copyright", "@anytime", "@theforefront",
    "@homeandabroad", "@thistime", "@www", "pleasefeel"
]


@dataclass
class SupplierRecord:
    company_name: str
    website_url: str = ""
    country: str = ""
    email: str = ""
    source_directory: str = SOURCE_DIRECTORY
    profile_url: str = ""
    company_description: str = ""
    is_target_supplier: bool = False
    confidence: float = 0.0
    ai_reason: str = ""
    ai_target_keywords: str = ""


def random_delay() -> None:
    time.sleep(random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))


def mic_site_gap() -> None:
    jitter = (
        random.uniform(0, MIC_SITE_GAP_JITTER_SECONDS)
        if MIC_SITE_GAP_JITTER_SECONDS > 0
        else 0.0
    )
    time.sleep(max(0.0, MIC_SITE_GAP_SECONDS + jitter))


def strip_tags(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", unescape(text))).strip()


def extract_main_products_description(html: str) -> str:
    """Extract Made-in-China 'Main Products' from the company profile."""
    if not html:
        return ""
    direct_pattern = (
        r'<div[^>]*class=["\'][^"\']*\bsr-comProfile-label\b[^"\']*["\'][^>]*>\s*'
        r'Main\s+Products:?\s*</div>\s*'
        r'<div[^>]*class=["\'][^"\']*\bsr-comProfile-fields\b[^"\']*["\'][^>]*>(.*?)</div>'
    )
    match = re.search(direct_pattern, html, re.I | re.S)
    if match:
        text = strip_tags(match.group(1))
        if text:
            return f"Main Products: {text}"
    item_pattern = r'<div[^>]*class=["\'][^"\']*\bsr-comProfile-item\b[^"\']*["\'][^>]*>(.*?)</div>\s*</div>'
    for item_html in re.findall(item_pattern, html, re.I | re.S):
        if "main products" not in strip_tags(item_html).lower():
            continue
        fields_match = re.search(
            r'<div[^>]*class=["\'][^"\']*\bsr-comProfile-fields\b[^"\']*["\'][^>]*>(.*?)</div>',
            item_html,
            re.I | re.S,
        )
        if fields_match:
            text = strip_tags(fields_match.group(1))
            if text:
                return f"Main Products: {text}"
    return ""


def normalize_url(url: str) -> str:
    value = (url or "").strip()
    if not value:
        return ""
    if value.startswith("https://") or value.startswith("http://"):
        return value
    if value.startswith("//"):
        return f"https:{value}"
    if value.startswith("/"):
        return f"{BASE_DOMAIN}{value}"
    return f"{BASE_DOMAIN}/{value.lstrip('/')}"


def is_plausible_external_website(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.netloc or "").lower()
    if not host:
        return False
    blocked = (
        "made-in-china.com", "trademessenger.com", "micen.com",
        "facebook.com", "instagram.com", "linkedin.com", "youtube.com", "wa.me",
    )
    return not any(token in host for token in blocked)


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
    clean = re.sub(r"<[^>]+>", " ", text)
    clean = re.sub(r"\s*\(at\)\s*|\s*\[at\]\s*|\s+at\s+", "@", clean, flags=re.I)
    clean = re.sub(r"\s*\(dot\)\s*|\s*\[dot\]\s*|\s+dot\s+", ".", clean, flags=re.I)
    clean = re.sub(r"\s+", "", clean)
    match = re.search(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", clean)
    return match.group(0) if match else None


def is_useful_email(email: str) -> bool:
    if not email or len(email) > 50 or len(email) < 6:
        return False
    if ' ' in email or email.count('@') != 1:
        return False
    if not re.search(r'\.(com|net|org|cn|kr|jp|tw|vn|th|sg|my|hk|co\.\w+)$', email):
        return False
    domain = email.split("@")[-1].lower()
    blocked = ("made-in-china.com", "micstatic.com", "focus.cn", "alibaba.com")
    if any(domain == d or domain.endswith(f".{d}") for d in blocked):
        return False
    if any(j in email.lower() for j in JUNK_EMAIL_PHRASES):
        return False
    return True


def initialize_fetcher() -> ScraplingStealthFetcher:
    try:
        ScraplingStealthFetcher.configure(browser_engine="camoufox")
    except Exception:
        pass
    return ScraplingStealthFetcher()


def fetch_html(fetcher: ScraplingStealthFetcher, url: str) -> tuple[int, str]:
    try:
        response = fetcher.fetch(url, headers=DEFAULT_HEADERS)
        status_code = getattr(response, "status", None) or getattr(response, "status_code", 200)
        body = getattr(response, "body", None)
        if isinstance(body, (bytes, bytearray)):
            return int(status_code), body.decode("utf-8", errors="ignore")
        if isinstance(body, str) and body:
            return int(status_code), body
        if hasattr(response, "text") and response.text:
            return int(status_code), response.text
        return int(status_code), str(response)
    except Exception:
        return 0, ""


def fetch_page_requests(url: str, timeout: int = WEBSITE_TIMEOUT) -> Optional[str]:
    try:
        resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
        return None
    except Exception:
        return None


def extract_email_from_html(html: str) -> Optional[str]:
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
            email = extract_email_from_text_flexible(match.group(1))
            email = clean_email(email)
            if email and is_useful_email(email):
                return email
    
    email = extract_email_from_text_flexible(text)
    email = clean_email(email)
    if email and is_useful_email(email):
        return email
    return None


def lookup_email_requests(website_url: str) -> str:
    if not website_url:
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


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")


def build_search_urls(keyword: str, page: int = 1) -> list[str]:
    if page == 1:
        encoded_keyword = quote_plus(keyword)
        return [(
            f"{BASE_DOMAIN}/companysearch.do"
            f"?subaction=hunt&style=b&mode=and&code=0&comProvince=nolimit"
            f"&order=0&isOpenCorrection=1&org=&keyword=&file=&searchType=1"
            f"&word={encoded_keyword}"
        )]
    encoded_path_keyword = quote(keyword)
    return [f"{BASE_DOMAIN}/company-search/{encoded_path_keyword}/C1/{page}.html"]


def to_deduped_dataframe(records: list[SupplierRecord]) -> pd.DataFrame:
    df = pd.DataFrame([asdict(r) for r in records])
    if df.empty:
        return df
    df["country"] = DEFAULT_COUNTRY
    df["company_name_norm"] = (
        df["company_name"].fillna("").astype(str).str.strip().str.lower().str.replace(r"\s+", " ", regex=True)
    )
    return df.drop_duplicates(subset=["company_name_norm"]).drop(columns=["company_name_norm"])


def save_scrape_checkpoint(records: list[SupplierRecord], output_path: str) -> None:
    df = to_deduped_dataframe(records)
    if not df.empty:
        df.to_csv(output_path, index=False, encoding="utf-8-sig")


def save_enrich_checkpoint(records: list[SupplierRecord], output_path: str) -> None:
    df = to_deduped_dataframe(records)
    if not df.empty:
        df.to_csv(output_path, index=False, encoding="utf-8-sig")


def filter_records_with_company_websites(records: list[SupplierRecord]) -> list[SupplierRecord]:
    kept = [record for record in records if record.website_url and is_plausible_external_website(record.website_url)]
    removed = len(records) - len(kept)
    print(f"[CLEANUP] Keeping {len(kept)} records with company websites; removed {removed} before email enrichment.")
    return kept


def parse_company_anchors(html: str) -> list[dict[str, str]]:
    anchors: list[dict[str, str]] = []
    seen_links: set[str] = set()
    for href, inner in re.findall(
        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html, flags=re.I | re.S,
    ):
        full_url = normalize_url(href)
        parsed = urlparse(full_url)
        host = (parsed.netloc or "").lower()
        if not host.endswith(".en.made-in-china.com"):
            continue
        if "/product/" in parsed.path.lower() or "/redirect.do" in parsed.path.lower():
            continue
        company_name = strip_tags(inner)
        if len(company_name) < 2 or company_name.lower() in {"contact us", "learn more"}:
            continue
        clean_profile = f"https://{host}"
        if clean_profile in seen_links:
            continue
        seen_links.add(clean_profile)
        anchors.append({"href": clean_profile, "name": company_name})
    return anchors


def extract_company_record(anchor: dict[str, str], keyword: str) -> SupplierRecord:
    return SupplierRecord(
        company_name=anchor.get("name", "").strip() or "Unknown Supplier",
        country=DEFAULT_COUNTRY,
        profile_url=anchor.get("href", "").strip(),
    )


def extract_contact_page_url(profile_html: str, profile_url: str) -> str:
    for href, text in re.findall(
        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', profile_html, flags=re.I | re.S,
    ):
        label = strip_tags(text).lower()
        normalized = normalize_url(href)
        if "contact-info.html" in normalized.lower():
            return normalized
        if "contact" in label and urlparse(normalized).netloc == urlparse(profile_url).netloc:
            return normalized
    return f"{profile_url.rstrip('/')}/contact-info.html"


def extract_about_page_url(profile_html: str, profile_url: str) -> str:
    same_host_company_links = []
    for href, text in re.findall(
        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', profile_html, flags=re.I | re.S,
    ):
        label = strip_tags(text).lower()
        normalized = normalize_url(href)
        if urlparse(normalized).netloc != urlparse(profile_url).netloc:
            continue
        if "about us" in label:
            return normalized
        if "/company-" in normalized.lower():
            same_host_company_links.append(normalized)
    return same_host_company_links[0] if same_host_company_links else ""


def extract_about_page_candidates(profile_html: str, profile_url: str) -> list[str]:
    candidates = []
    primary = extract_about_page_url(profile_html, profile_url)
    if primary:
        candidates.append(primary)
    profile_host = urlparse(profile_url).netloc
    for href in re.findall(r'href=["\']([^"\']*company-[^"\']+\.html)["\']', profile_html, re.I):
        normalized = normalize_url(href)
        if urlparse(normalized).netloc == profile_host and normalized not in candidates:
            candidates.append(normalized)
    return candidates


def extract_website_link_from_contact_html(contact_html: str) -> str:
    match = re.search(
        r'<a[^>]+class=["\'][^"\']*\blink-web\b[^"\']*["\'][^>]+href=["\']([^"\']+)["\']',
        contact_html, flags=re.I,
    )
    if match:
        return normalize_url(match.group(1))
    redirect_match = re.search(
        r'href=["\']([^"\']*redirect\.do\?[^"\']*action=com[^"\']*)["\']',
        contact_html, flags=re.I,
    )
    if redirect_match:
        return normalize_url(redirect_match.group(1))
    return ""


def resolve_redirect_target(fetcher: ScraplingStealthFetcher, url: str) -> str:
    if not url:
        return ""
    candidate = normalize_url(url)
    if is_plausible_external_website(candidate):
        return candidate
    try:
        headers = dict(DEFAULT_HEADERS)
        headers["Referer"] = BASE_DOMAIN
        request = Request(candidate, headers=headers)
        with urlopen(request, timeout=25) as response:
            final_url = response.geturl().strip()
            if is_plausible_external_website(final_url):
                return final_url
    except Exception:
        pass
    return ""


def _mic_contact_headless() -> bool:
    raw = os.getenv("MIC_CONTACT_HEADLESS")
    if raw is None or str(raw).strip() == "":
        return False
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _find_mic_website_link(page):
    selectors = (
        "a.link-web", "a[class*='link-web']",
        'a[href*="redirect.do"][href*="action=com"]', "a[href*='redirect.do']",
    )
    for sel in selectors:
        loc = page.locator(sel).first
        try:
            if loc.count() > 0 and loc.is_visible(timeout=2000):
                return loc
        except Exception:
            continue
    try:
        loc = page.get_by_role("link", name=re.compile(r"website|web\s*site", re.I)).first
        if loc.count() > 0 and loc.is_visible(timeout=1500):
            return loc
    except Exception:
        pass
    return None


def _capture_website_url_after_click(page) -> str:
    loc = _find_mic_website_link(page)
    if loc is None:
        return ""
    target_page = None
    used_popup = False
    try:
        with page.context.expect_page(timeout=20000) as new_page_info:
            loc.click(timeout=12000)
        target_page = new_page_info.value
        used_popup = target_page is not page
    except (PlaywrightTimeoutError, Exception):
        try:
            with page.expect_navigation(timeout=20000, wait_until="domcontentloaded"):
                loc.click(timeout=12000, force=True)
            target_page = page
        except Exception:
            return ""
    if target_page is None:
        return ""
    try:
        target_page.wait_for_load_state("domcontentloaded", timeout=25000)
    except Exception:
        pass
    try:
        u = (target_page.url or "").strip()
        if is_plausible_external_website(u):
            if used_popup and target_page is not page:
                try:
                    target_page.close()
                except Exception:
                    pass
            return u
    except Exception:
        pass
    if used_popup and target_page is not page:
        try:
            target_page.close()
        except Exception:
            pass
    return ""


class MicContactPlaywrightSession:
    def __init__(self) -> None:
        self._playwright = None
        self._context = None
        self._page = None

    @property
    def page(self):
        return self._page

    def __enter__(self) -> "MicContactPlaywrightSession":
        if sync_playwright is None:
            raise RuntimeError("playwright is not installed")
        self._playwright = sync_playwright().start()
        user_data = str(Path(MIC_CONTACT_PROFILE_DIR).resolve())
        launch_kwargs: dict = dict(
            user_data_dir=user_data,
            headless=_mic_contact_headless(),
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            args=PLAYWRIGHT_CONTACT_EXTRA_ARGS,
        )
        if PLAYWRIGHT_CONTACT_CHANNEL:
            launch_kwargs["channel"] = PLAYWRIGHT_CONTACT_CHANNEL
        try:
            self._context = self._playwright.chromium.launch_persistent_context(**launch_kwargs)
        except Exception as exc:
            if launch_kwargs.pop("channel", None):
                print(f"[CONTACT][WARN] Could not launch channel '{PLAYWRIGHT_CONTACT_CHANNEL}': {exc}")
                self._context = self._playwright.chromium.launch_persistent_context(**launch_kwargs)
            else:
                raise
        self._page = self._context.new_page()
        return self

    def __exit__(self, *args) -> None:
        try:
            if self._context:
                self._context.close()
        finally:
            if self._playwright:
                self._playwright.stop()


def enrich_from_contact_page_playwright(page, record: SupplierRecord) -> None:
    profile_url = (record.profile_url or "").strip()
    if not profile_url or not page:
        return
    if record.website_url and record.email and record.company_description:
        return
    timeout = max(10_000, int(CONTACT_ENRICHMENT_BROWSER_TIMEOUT_MS))
    try:
        page.goto(profile_url, wait_until="domcontentloaded", timeout=timeout)
    except Exception:
        return
    page.wait_for_timeout(800)
    try:
        profile_html = page.content()
    except Exception:
        return
    description = extract_main_products_description(profile_html)
    if description and not record.company_description:
        record.company_description = description
    for about_url in extract_about_page_candidates(profile_html, profile_url):
        if record.company_description:
            break
        mic_site_gap()
        try:
            page.goto(about_url, wait_until="domcontentloaded", timeout=timeout)
            page.wait_for_timeout(600)
            about_html = page.content()
            description = extract_main_products_description(about_html)
            if description:
                record.company_description = description
        except Exception:
            pass
    contact_url = extract_contact_page_url(profile_html, profile_url)
    mic_site_gap()
    try:
        page.goto(contact_url, wait_until="domcontentloaded", timeout=timeout)
    except Exception:
        return
    page.wait_for_timeout(600)
    if not record.website_url:
        resolved = _capture_website_url_after_click(page)
        if resolved:
            record.website_url = resolved


def enrich_from_contact_page(fetcher: ScraplingStealthFetcher, record: SupplierRecord) -> None:
    profile_url = (record.profile_url or "").strip()
    if not profile_url:
        return
    if record.website_url and record.email and record.company_description:
        return
    try:
        _, profile_html = fetch_html(fetcher, profile_url)
    except Exception:
        return
    description = extract_main_products_description(profile_html)
    if description and not record.company_description:
        record.company_description = description
    for about_url in extract_about_page_candidates(profile_html, profile_url):
        if record.company_description:
            break
        mic_site_gap()
        try:
            _, about_html = fetch_html(fetcher, about_url)
            description = extract_main_products_description(about_html)
            if description:
                record.company_description = description
        except Exception:
            pass
    contact_url = extract_contact_page_url(profile_html, profile_url)
    mic_site_gap()
    try:
        _, contact_html = fetch_html(fetcher, contact_url)
    except Exception:
        return
    if not record.website_url:
        redirect_or_site = extract_website_link_from_contact_html(contact_html)
        resolved_site = resolve_redirect_target(fetcher, redirect_or_site)
        if resolved_site:
            record.website_url = resolved_site


def _run_contact_loop_playwright(page, records: list[SupplierRecord]) -> None:
    attempted = 0
    updated = 0
    new_websites_since_checkpoint = 0
    for record in records:
        if MAX_CONTACT_ENRICHMENTS > 0 and attempted >= MAX_CONTACT_ENRICHMENTS:
            print(f"[CONTACT] Reached contact enrichment cap ({MAX_CONTACT_ENRICHMENTS}).")
            break
        if not record.profile_url:
            continue
        if record.website_url and record.email and record.company_description:
            continue
        before = (record.website_url, record.email)
        had_site_before = bool((record.website_url or "").strip())
        try:
            enrich_from_contact_page_playwright(page, record)
        except Exception as exc:
            print(f"[CONTACT][WARN] {record.company_name[:48]}: {exc!r}")
        attempted += 1
        if (record.website_url, record.email) != before:
            updated += 1
        if not had_site_before and (record.website_url or "").strip():
            new_websites_since_checkpoint += 1
            if new_websites_since_checkpoint >= AUTOSAVE_EVERY_NEW_RECORDS:
                save_scrape_checkpoint(records, PARTIAL_SCRAPE_CSV)
                print(f"[CHECKPOINT] Saved {len(records)} records ({AUTOSAVE_EVERY_NEW_RECORDS} new websites)")
                new_websites_since_checkpoint = 0
        if attempted % 10 == 0:
            print(f"[CONTACT] Processed {attempted} profiles; updated {updated}.")
        mic_site_gap()


def run_contact_page_enrichment(records: list[SupplierRecord]) -> None:
    if not ENABLE_CONTACT_PAGE_ENRICHMENT:
        return
    use_pw = CONTACT_ENRICHMENT_USE_PLAYWRIGHT and sync_playwright is not None
    if use_pw:
        try:
            print("[CONTACT] Starting Playwright contact-page enrichment...")
            with MicContactPlaywrightSession() as session:
                _run_contact_loop_playwright(session.page, records)
            print("[CONTACT] Playwright contact enrichment done.")
            return
        except Exception as exc:
            print(f"[CONTACT][WARN] Playwright session failed ({exc!r}); using HTTP fallback.")
    fetcher = initialize_fetcher()
    attempted = 0
    updated = 0
    new_websites_since_checkpoint = 0
    for record in records:
        if MAX_CONTACT_ENRICHMENTS > 0 and attempted >= MAX_CONTACT_ENRICHMENTS:
            print(f"[CONTACT] Reached contact enrichment cap ({MAX_CONTACT_ENRICHMENTS}).")
            break
        if not record.profile_url:
            continue
        if record.website_url and record.email and record.company_description:
            continue
        before = (record.website_url, record.email)
        had_site_before = bool((record.website_url or "").strip())
        enrich_from_contact_page(fetcher, record)
        attempted += 1
        if (record.website_url, record.email) != before:
            updated += 1
        if not had_site_before and (record.website_url or "").strip():
            new_websites_since_checkpoint += 1
            if new_websites_since_checkpoint >= AUTOSAVE_EVERY_NEW_RECORDS:
                save_scrape_checkpoint(records, PARTIAL_SCRAPE_CSV)
                print(f"[CHECKPOINT] Saved {len(records)} records ({AUTOSAVE_EVERY_NEW_RECORDS} new websites)")
                new_websites_since_checkpoint = 0
        if attempted % 10 == 0:
            print(f"[CONTACT] Processed {attempted} profiles; updated {updated}.")
        mic_site_gap()


def enrich_emails_from_company_websites(
    records: list[SupplierRecord],
    max_lookups: int = MAX_WEBSITE_EMAIL_LOOKUPS,
) -> None:
    if not ENABLE_WEBSITE_EMAIL_ENRICHMENT:
        return
    candidates = [r for r in records if r.website_url and not r.email]
    if max_lookups > 0:
        candidates = candidates[:max_lookups]
    if not candidates:
        print("  No companies need email enrichment")
        return

    print(f"\n[EMAILS] Looking for emails on {len(candidates)} company websites...")
    print(f"  Strategy: {len(CONTACT_PATHS)} contact page variations | Timeout: {WEBSITE_TIMEOUT}s | Footer-first")

    found = 0
    skipped = 0
    emails_since_checkpoint = 0

    with ThreadPoolExecutor(max_workers=WEBSITE_EMAIL_WORKERS) as executor:
        futures = {executor.submit(lookup_email_requests, r.website_url): r for r in candidates}
        
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
                emails_since_checkpoint += 1
                
                if emails_since_checkpoint >= AUTOSAVE_EVERY_NEW_EMAILS:
                    save_enrich_checkpoint(records, PARTIAL_ENRICH_CSV)
                    print(f"  [ENRICH SAVE] {found} emails → {PARTIAL_ENRICH_CSV}")
                    emails_since_checkpoint = 0
            
            if i % 50 == 0:
                print(f"  Progress: {i}/{len(candidates)}, found {found}, skipped {skipped}")

    save_enrich_checkpoint(records, PARTIAL_ENRICH_CSV)
    print(f"  [ENRICH DONE] {found} emails found, {skipped} timed out. Saved to {PARTIAL_ENRICH_CSV}")


def scrape_made_in_china() -> pd.DataFrame:
    fetcher = initialize_fetcher()
    all_records: list[SupplierRecord] = []
    seen_company_names: set[str] = set()
    records_since_last_autosave = 0

    print("=" * 60)
    print("Made-in-China Cosmetic Packaging Supplier Scraper")
    print(f"Target: {TARGET_SUPPLIERS} | Empty cutoff: {ZERO_NEW_PAGES_CUTOFF}")
    print(f"Email timeout: {WEBSITE_TIMEOUT}s | Contact paths: {len(CONTACT_PATHS)}")
    print("=" * 60)

    try:
        for keyword in KEYWORDS:
            if len(all_records) >= TARGET_SUPPLIERS:
                print(f"[INFO] Stopping at {TARGET_SUPPLIERS} records.")
                break
            
            consecutive_zero_new_pages = 0
            for page in range(1, MAX_PAGES_PER_QUERY + 1):
                if len(all_records) >= TARGET_SUPPLIERS:
                    break
                
                html = ""
                status_code = 0
                for url in build_search_urls(keyword, page):
                    try:
                        status_code, html = fetch_html(fetcher, url)
                    except Exception:
                        continue
                    if status_code == 200 and html:
                        break

                if status_code != 200 or not html:
                    consecutive_zero_new_pages += 1
                    print(f"[WARN] Page {page} failed ({consecutive_zero_new_pages}/{ZERO_NEW_PAGES_CUTOFF})")
                    if consecutive_zero_new_pages >= ZERO_NEW_PAGES_CUTOFF:
                        print(f"[SKIP] {consecutive_zero_new_pages} consecutive bad pages. Moving on.")
                        break
                    mic_site_gap()
                    continue

                anchors = parse_company_anchors(html)
                if not anchors:
                    consecutive_zero_new_pages += 1
                    print(f"[INFO] Page {page} - 0 suppliers ({consecutive_zero_new_pages}/{ZERO_NEW_PAGES_CUTOFF})")
                    if consecutive_zero_new_pages >= ZERO_NEW_PAGES_CUTOFF:
                        print(f"[SKIP] {consecutive_zero_new_pages} consecutive empty pages. Moving on.")
                        break
                    mic_site_gap()
                    continue

                consecutive_zero_new_pages = 0
                new_count = 0
                for anchor in anchors:
                    if len(all_records) >= TARGET_SUPPLIERS:
                        break
                    
                    record = extract_company_record(anchor, keyword)
                    normalized_name = re.sub(r"\s+", " ", record.company_name).strip().lower()
                    if not normalized_name or normalized_name in seen_company_names:
                        continue
                    seen_company_names.add(normalized_name)
                    if PROFILE_ENRICH_DURING_SCRAPE:
                        enrich_from_contact_page(fetcher, record)
                    all_records.append(record)
                    new_count += 1
                    records_since_last_autosave += 1
                    if records_since_last_autosave >= AUTOSAVE_EVERY_NEW_RECORDS:
                        save_scrape_checkpoint(all_records, PARTIAL_SCRAPE_CSV)
                        print(f"[SCRAPE SAVE] {len(all_records)} records → {PARTIAL_SCRAPE_CSV}")
                        records_since_last_autosave = 0

                print(f"Page {page} ({keyword}): +{new_count} (Total: {len(all_records)})")
                mic_site_gap()

                if len(all_records) >= TARGET_SUPPLIERS:
                    print(f"[INFO] Target reached: {TARGET_SUPPLIERS} supplier records.")
                    break
            
            if len(all_records) >= TARGET_SUPPLIERS:
                break
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Saving progress...")
        save_scrape_checkpoint(all_records, PARTIAL_SCRAPE_CSV)
        print(f"[SAVED] {len(all_records)} records to {PARTIAL_SCRAPE_CSV}")
        return to_deduped_dataframe(all_records)

    save_scrape_checkpoint(all_records, PARTIAL_SCRAPE_CSV)
    print(f"\n[PHASE 1 DONE] {len(all_records)} companies scraped. Saved to {PARTIAL_SCRAPE_CSV}")

    if all_records and ENABLE_CONTACT_PAGE_ENRICHMENT:
        print("\n[PHASE 2] Starting contact-page enrichment...")
        run_contact_page_enrichment(all_records)
        save_scrape_checkpoint(all_records, PARTIAL_SCRAPE_CSV)
        print(f"[PHASE 2 DONE] Contact-page enrichment complete.")

    if all_records:
        all_records = filter_records_with_company_websites(all_records)
        save_enrich_checkpoint(all_records, PARTIAL_ENRICH_CSV)

    # AI filtering disabled: go directly from website filtering to email enrichment.
    # if all_records and ENABLE_AI_FILTERING:
    #     print("\n[PHASE 3] Starting AI filtering...")
    #     all_records = apply_ai_filter_to_records(
    #         all_records,
    #         keywords=KEYWORDS,
    #         source_name=SOURCE_DIRECTORY,
    #         verified_csv=AI_VERIFIED_CSV,
    #         rejected_csv=AI_REJECTED_CSV,
    #         checkpoint_csv=AI_CHECKPOINT_CSV,
    #         batch_size=AI_BATCH_SIZE,
    #         concurrent=AI_CONCURRENT,
    #     )
    #     save_enrich_checkpoint(all_records, PARTIAL_ENRICH_CSV)

    if all_records and ENABLE_WEBSITE_EMAIL_ENRICHMENT:
        print("\n[PHASE 4] Starting email enrichment for companies with websites...")
        enrich_emails_from_company_websites(all_records)
        save_enrich_checkpoint(all_records, PARTIAL_ENRICH_CSV)

    return to_deduped_dataframe(all_records)


def main() -> None:
    df = scrape_made_in_china()
    if df.empty:
        print("[INFO] No records found.")
        return
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    df_clean = df[df['email'].notna() & (df['email'] != '')]
    df_clean.to_csv(CLEANED_CSV, index=False, encoding="utf-8-sig")

    print(f"\n{'='*60}")
    print(f"ALL DONE!")
    print(f"  Scrape progress: {PARTIAL_SCRAPE_CSV}")
    print(f"  Enrich progress: {PARTIAL_ENRICH_CSV}")
    print(f"  Raw: {OUTPUT_CSV} ({len(df)} suppliers)")
    print(f"  With website: {(df['website_url'] != '').sum()}")
    print(f"  With email: {(df['email'] != '').sum()}")
    print(f"  Cleaned: {CLEANED_CSV} ({len(df_clean)} suppliers)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
