"""
Ensun.io supplier scraper - Playwright-based (JavaScript-rendered SPA)
Uses Made-in-China approach with browser automation and stealth.
With junk website filtering to ensure only legitimate business websites are captured.
Produces the same CSV format as the Made-in-China script.
"""

from __future__ import annotations

import random
import re
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from dataclasses import dataclass, asdict
from html import unescape
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from scraper_runtime_config import env_int, env_list

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None
    PlaywrightTimeoutError = Exception


KEYWORDS = [
    # TUBES
    # "cosmetic tube",
    # "squeeze tube cosmetic",
    # "laminated tube cosmetic",
    # "plastic cosmetic tube",
    # "hand cream tube",
    # "lip balm tube",
    # "lotion tube packaging",
    # "bb cream tube",
  
    # JARS & CONTAINERS
    # "cosmetic jar",
    # "cream jar packaging",
    # "skincare jar supplier",
    # "glass cosmetic jar",
    # "airless jar cosmetic",
    
    # BOTTLES
    "cosmetic bottle supplier",
    "lotion bottle packaging",
    "serum bottle cosmetic",
    "airless bottle cosmetic",
    "pump bottle cosmetic",
    
    # # PUMPS & DISPENSERS
    "lotion pump dispenser",
    "airless pump bottle",
    "cosmetic pump packaging",
    "foam pump cosmetic",
    
    # # CAPS & CLOSURES
    # "cosmetic cap supplier",
    # "jar cap packaging",
    # "bottle cap cosmetic",
    # "flip top cap cosmetic",
    # "disc top cap",
    # "plastic closure cosmetic",
    # "PP cap supplier",
    
    # # GENERAL PACKAGING
    # "cosmetic packaging supplier",
    # "skincare packaging manufacturer",
    # "beauty packaging supplier",
    # "primary packaging cosmetics",
    # "cosmetic packaging OEM",
   
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

SOURCE_DIRECTORY = "Ensun"
BASE_DOMAIN = "https://ensun.io"
MAX_PAGES_PER_QUERY = 50
MIN_DELAY_SECONDS = 1
MAX_DELAY_SECONDS = 2
# Match Made-in-China naming convention
OUTPUT_CSV = "ensun_suppliers_phase1_raw.csv"
CLEANED_CSV = "ensun_suppliers_cleaned.csv"
PARTIAL_SCRAPE_CSV = "ensun_suppliers_partial_scrape.csv"
PARTIAL_ENRICH_CSV = "ensun_suppliers_partial_enrichment.csv"
TARGET_SUPPLIERS = 2000
AUTOSAVE_EVERY_NEW_RECORDS = 10
AUTOSAVE_EVERY_NEW_EMAILS = 1

# SEARCH CONFIGURATION
SEARCH_THRESHOLD = "VERY_LOW"
PERMANENT_FILTERS = {
    "categories": ["MANUFACTURER", "DISTRIBUTOR"],
}

# PLAYWRIGHT CONFIGURATION
USE_BROWSER_FOR_SEARCH = True
SEARCH_BROWSER_HEADLESS = False
SEARCH_BROWSER_TIMEOUT_SECONDS = 60
ENSUN_PROFILE_DIR = ".ensun_playwright_profile"
PLAYWRIGHT_CHANNEL = "chrome"
PLAYWRIGHT_EXTRA_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-features=IsolateOrigins,site-per-process",
    "--no-sandbox",
]
VERCEL_WAIT_SECONDS = 6
PAGE_LOAD_WAIT_SECONDS = 1
PROFILE_LOAD_WAIT_SECONDS = 1
COMPANY_CARD_WAIT_SECONDS = 15
FETCH_PROFILE_DETAILS_IN_PHASE1 = True
FILTER_RECORDS_WITH_WEBSITES = True

# WEBSITE EMAIL ENRICHMENT
ENABLE_WEBSITE_EMAIL_ENRICHMENT = True
MAX_WEBSITE_EMAIL_LOOKUPS = 0
WEBSITE_EMAIL_WORKERS = 10
WEBSITE_TIMEOUT = 15

# JUNK WEBSITE FILTERING
JUNK_WEBSITES = [
    "example.com", "yourcompany.com", "company.com", "website.com",
    "yourwebsite.com", "mysite.com", "test.com", "sample.com",
    "demo.com", "placeholder.com", "domain.com", "site.com",
    "mycompany.com", "business.com", "none.com", "nosite.com",
    "nowebsite.com", "underconstruction.com", "comingsoon.com",
]

JUNK_WEBSITE_PATTERNS = [
    r"^https?://(www\.)?example\.com",
    r"^https?://(www\.)?yourcompany\.com",
    r"^https?://(www\.)?company\.com",
    r"^https?://(www\.)?test\.com",
    r"^https?://(www\.)?sample\.com",
    r"^https?://(www\.)?demo\.com",
    r"^https?://(www\.)?placeholder\.com",
    r"^https?://(www\.)?mysite\.com",
    r"^https?://(www\.)?website\.com",
    r"^https?://(www\.)?none\.com",
    r"^https?://(www\.)?nosite\.com",
    r"^https?://(www\.)?domain\.com",
    r"^https?://(www\.)?site\.com",
    r"^https?://(www\.)?mycompany\.com",
    r"^https?://(www\.)?business\.com",
]

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

KEYWORDS = env_list("SCRAPER_KEYWORDS", KEYWORDS)
COUNTRIES = env_list("SCRAPER_COUNTRIES", COUNTRIES)
TARGET_SUPPLIERS = env_int("SCRAPER_TARGET_SUPPLIERS", TARGET_SUPPLIERS)
MAX_WEBSITE_EMAIL_LOOKUPS = env_int("ENSUN_MAX_EMAIL_LOOKUPS", MAX_WEBSITE_EMAIL_LOOKUPS)


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


def clean_email(email: str) -> Optional[str]:
    if not email:
        return None
    match = re.match(r'([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})([A-Z].*)?', email)
    if match:
        return match.group(1)
    return email


def is_useful_email(email: str) -> bool:
    if not email or len(email) > 50 or len(email) < 6:
        return False
    if ' ' in email or email.count('@') != 1:
        return False
    if not re.search(r'\.(com|net|org|cn|kr|jp|tw|vn|th|sg|my|hk|co\.\w+)$', email):
        return False
    blocked = ["ensun.io", "alibaba.com", "made-in-china.com", "tradewheel.com"]
    if any(b in email.lower() for b in blocked):
        return False
    if any(j in email.lower() for j in JUNK_EMAIL_PHRASES):
        return False
    return True


def is_junk_website(url: str) -> bool:
    if not url:
        return False
    url_lower = url.lower()
    for junk in JUNK_WEBSITES:
        if junk in url_lower:
            return True
    for pattern in JUNK_WEBSITE_PATTERNS:
        if re.match(pattern, url_lower):
            return True
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.replace("www.", "")
        if domain.count('.') == 0:
            return True
        if len(domain) < 6:
            return True
    except:
        pass
    return False


def is_plausible_website(url: str) -> bool:
    if not url:
        return False
    if is_junk_website(url):
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.netloc or "").lower()
    if not host or "ensun.io" in host:
        return False
    blocked = (
        "facebook.com", "instagram.com", "linkedin.com", 
        "youtube.com", "wa.me", "twitter.com", "tiktok.com",
        "pinterest.com", "reddit.com", "telegram.org",
    )
    if any(t in host for t in blocked):
        return False
    if host.count('.') < 1:
        return False
    if len(host) < 7:
        return False
    return True


def is_valid_profile_url(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(urljoin(BASE_DOMAIN, url))
    if parsed.netloc and parsed.netloc != urlparse(BASE_DOMAIN).netloc:
        return False
    return parsed.path.startswith("/company/")


def normalize_profile_url(url: str) -> str:
    if not url:
        return ""
    full_url = urljoin(BASE_DOMAIN, url)
    return full_url if is_valid_profile_url(full_url) else ""


def build_search_url(keyword: str, country: str = "", page: int = 1) -> str:
    params = [
        f"threshold={SEARCH_THRESHOLD}",
        f"q={keyword}",
    ]
    if page > 1:
        params.append(f"page={page}")
    if country:
        params.append(f"locations={country},null,null")
    for category in PERMANENT_FILTERS["categories"]:
        params.append(f"categories={category}")
    return f"{BASE_DOMAIN}/search?{'&'.join(params)}"


class EnsunBrowser:
    def __init__(self, timeout_seconds: int = SEARCH_BROWSER_TIMEOUT_SECONDS):
        self.timeout_ms = max(10, int(timeout_seconds)) * 1000
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.enabled = sync_playwright is not None and USE_BROWSER_FOR_SEARCH

    def __enter__(self):
        if not self.enabled:
            return self
        print("[BROWSER] Launching Playwright browser...")
        self.playwright = sync_playwright().start()
        launch_kwargs = dict(
            user_data_dir=str(Path(ENSUN_PROFILE_DIR).resolve()),
            headless=SEARCH_BROWSER_HEADLESS,
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="Asia/Shanghai",
            args=PLAYWRIGHT_EXTRA_ARGS,
        )
        if PLAYWRIGHT_CHANNEL:
            launch_kwargs["channel"] = PLAYWRIGHT_CHANNEL
        try:
            self.context = self.playwright.chromium.launch_persistent_context(**launch_kwargs)
        except Exception as exc:
            if launch_kwargs.pop("channel", None):
                print(f"[BROWSER][WARN] Could not launch channel '{PLAYWRIGHT_CHANNEL}': {exc}")
                self.context = self.playwright.chromium.launch_persistent_context(**launch_kwargs)
            else:
                raise
        self.page = self.context.new_page()
        self.page.set_extra_http_headers({
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        })
        print("[BROWSER] Browser ready")
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
        finally:
            if self.playwright:
                self.playwright.stop()

    def _wait_for_page_load(self):
        time.sleep(VERCEL_WAIT_SECONDS)
        try:
            self.page.wait_for_load_state("networkidle", timeout=15000)
        except:
            pass
        time.sleep(PAGE_LOAD_WAIT_SECONDS)
        self._wait_for_company_cards()

    def _wait_for_company_cards(self):
        try:
            self.page.wait_for_selector(
                "p.mui-1e9jes1",
                state="attached",
                timeout=COMPANY_CARD_WAIT_SECONDS * 1000,
            )
        except:
            pass

    def fetch_search_page(self, url: str) -> tuple[int, str]:
        if not self.enabled or self.page is None:
            return 0, ""
        try:
            self.page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
            self._wait_for_page_load()
            html = self._safe_page_content()
            if 'Vercel Security Checkpoint' in html:
                print("  [WARN] Hit Vercel Security Checkpoint, waiting longer...")
                time.sleep(15)
                html = self._safe_page_content()
            if '429' in html or 'Too Many Requests' in html:
                print("  [WARN] Rate limited! Waiting 30 seconds...")
                time.sleep(30)
                html = self._safe_page_content()
            return 200, html
        except PlaywrightTimeoutError:
            print("  [WARN] Timeout loading page")
            return 0, ""
        except Exception as e:
            print(f"  [WARN] Error: {e}")
            return 0, ""

    def _safe_page_content(self) -> str:
        last_error = None
        for _ in range(3):
            try:
                return self.page.content()
            except Exception as e:
                last_error = e
                time.sleep(1)
        raise last_error

    def extract_company_cards(self) -> list[dict[str, str]]:
        if not self.enabled or self.page is None:
            return []
        try:
            cards = self.page.evaluate(
                """() => Array.from(document.querySelectorAll('p.mui-1e9jes1'))
                    .map((el) => {
                        const name = (el.textContent || '').replace(/\\s+/g, ' ').trim();
                        const isProfileHref = (href) => href && href.includes('/company/');
                        const closestLink = el.closest('a[href]');
                        let link = isProfileHref(closestLink?.getAttribute('href') || '') ? closestLink : null;
                        let node = el.parentElement;
                        for (let depth = 0; !link && node && depth < 6; depth += 1) {
                            const links = Array.from(node.querySelectorAll('a[href]'));
                            link = links.find((candidate) => {
                                const href = candidate.getAttribute('href') || '';
                                return isProfileHref(href);
                            }) || null;
                            node = node.parentElement;
                        }
                        return {
                            name,
                            profile_url: link ? link.href : ''
                        };
                    })
                    .filter((card) => card.name)"""
            )
            seen = set()
            companies = []
            for card in cards:
                name = str(card.get("name", "")).strip()
                norm = re.sub(r"\s+", " ", name).lower()
                if not norm or norm in seen:
                    continue
                seen.add(norm)
                companies.append({
                    "name": name,
                    "profile_url": normalize_profile_url(str(card.get("profile_url", "")).strip()),
                })
            return companies
        except Exception as e:
            print(f"  [WARN] Card extraction error: {e}")
            return []

    def fetch_profile_page(self, url: str) -> tuple[int, str]:
        if not self.enabled or self.page is None:
            return 0, ""
        profile_url = normalize_profile_url(url)
        if not profile_url:
            return 0, ""
        try:
            self.page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(PROFILE_LOAD_WAIT_SECONDS)
            html = self._safe_page_content()
            return 200, html
        except Exception as e:
            print(f"    [WARN] Profile fetch error: {e}")
            return 0, ""

    def click_company_card_for_profile(self, search_url: str, company_name: str) -> tuple[int, str, str]:
        """Open a listing card by clicking the visible company title."""
        if not self.enabled or self.page is None:
            return 0, "", ""
        try:
            self.page.goto(search_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
            self._wait_for_page_load()
            title = self.page.locator("p.mui-1e9jes1", has_text=company_name).first
            if title.count() == 0:
                print(f"    [WARN] Could not find card to click: {company_name[:50]}")
                return 0, "", ""
            card = title.locator("xpath=ancestor::*[@onclick][1]").first
            click_target = card if card.count() > 0 else title
            before_url = self.page.url
            popup = None
            try:
                with self.page.expect_popup(timeout=5000) as popup_info:
                    click_target.click(timeout=7000)
                popup = popup_info.value
                popup.wait_for_load_state("domcontentloaded", timeout=15000)
            except Exception:
                try:
                    with self.page.expect_navigation(wait_until="domcontentloaded", timeout=15000):
                        click_target.click(timeout=7000)
                except Exception:
                    try:
                        click_target.click(timeout=7000, force=True)
                        self.page.wait_for_url(lambda current_url: current_url != before_url, timeout=15000)
                    except Exception:
                        pass
            time.sleep(PROFILE_LOAD_WAIT_SECONDS)
            profile_page = popup or self.page
            profile_url = normalize_profile_url(profile_page.url)
            if not profile_url:
                print(f"    [WARN] Card click did not navigate to a company page: {company_name[:50]}")
                if popup:
                    popup.close()
                return 0, "", ""
            profile_html = self._safe_page_content() if profile_page == self.page else profile_page.content()
            if popup:
                popup.close()
            return 200, profile_url, profile_html
        except Exception as e:
            print(f"    [WARN] Card click profile fetch error: {e}")
            return 0, "", ""

    def click_company_card_by_index(self, card_index: int, company_name: str) -> tuple[int, str, str]:
        """Click a card already visible on the current listing page."""
        if not self.enabled or self.page is None:
            return 0, "", ""
        try:
            titles = self.page.locator("p.mui-1e9jes1")
            title = titles.nth(card_index)
            if title.count() == 0:
                title = self.page.locator("p.mui-1e9jes1", has_text=company_name).first
            if title.count() == 0:
                print(f"    [WARN] Could not find card to click: {company_name[:50]}")
                return 0, "", ""

            card = title.locator("xpath=ancestor::*[@onclick][1]").first
            click_target = card if card.count() > 0 else title
            before_url = self.page.url
            popup = None
            try:
                with self.page.expect_popup(timeout=3000) as popup_info:
                    click_target.click(timeout=5000)
                popup = popup_info.value
                popup.wait_for_load_state("domcontentloaded", timeout=10000)
            except Exception:
                try:
                    with self.page.expect_navigation(wait_until="domcontentloaded", timeout=10000):
                        click_target.click(timeout=5000)
                except Exception:
                    try:
                        click_target.click(timeout=5000, force=True)
                        self.page.wait_for_url(lambda current_url: current_url != before_url, timeout=10000)
                    except Exception:
                        pass

            time.sleep(PROFILE_LOAD_WAIT_SECONDS)
            profile_page = popup or self.page
            profile_url = normalize_profile_url(profile_page.url)
            if not profile_url:
                print(f"    [WARN] Card click did not navigate to a company page: {company_name[:50]}")
                if popup:
                    popup.close()
                return 0, "", ""

            profile_html = self._safe_page_content() if profile_page == self.page else profile_page.content()
            if popup:
                popup.close()
            return 200, profile_url, profile_html
        except Exception as e:
            print(f"    [WARN] Card click profile fetch error: {e}")
            return 0, "", ""

    def return_to_listing(self, search_url: str) -> None:
        if not self.enabled or self.page is None:
            return
        if not normalize_profile_url(self.page.url):
            return
        try:
            self.page.go_back(wait_until="domcontentloaded", timeout=15000)
            time.sleep(PAGE_LOAD_WAIT_SECONDS)
        except Exception:
            self.page.goto(search_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
            self._wait_for_page_load()


def extract_company_links_from_search(html: str) -> list[dict[str, str]]:
    companies = []
    seen_names = set()
    name_pattern = re.compile(
        r'<p[^>]*class="[^"]*mui-1e9jes1[^"]*"[^>]*>(.*?)</p>',
        re.DOTALL | re.IGNORECASE,
    )
    anchor_pattern = re.compile(
        r'<a[^>]*href="(/[^"]*)"[^>]*>(.*?)</a>',
        re.DOTALL | re.IGNORECASE,
    )

    def add_company(raw_name: str, href: str = "") -> None:
        name = unescape(re.sub(r'<[^>]+>', '', raw_name)).strip()
        name = re.sub(r'\s+', ' ', name)
        if not name:
            return
        norm = name.lower()
        if norm in seen_names:
            return
        if len(name) < 3 or len(name) > 120:
            return
        if norm in {"search", "filter", "sort", "page", "next", "previous", "loading", "results"}:
            return
        if re.match(r'^[\d\s\W]+$', name):
            return
        seen_names.add(norm)
        profile_url = normalize_profile_url(href)
        companies.append({"name": name, "profile_url": profile_url})

    for href, anchor_html in anchor_pattern.findall(html):
        for match in name_pattern.findall(anchor_html):
            add_company(match, href)

    for match in name_pattern.findall(html):
        add_company(match)
    return companies


def extract_company_description(profile_html: str) -> str:
    desc_patterns = [
        r'<p[^>]*class="[^"]*mui-jqad34[^"]*"[^>]*>(.*?)</p>',
        r'<p[^>]*class="[^"]*MuiTypography-body1[^"]*"[^>]*>((?:(?!</p>).){100,})</p>',
        r'<div[^>]*class="[^"]*description[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*class="[^"]*about[^"]*"[^>]*>(.*?)</div>',
    ]
    for pattern in desc_patterns:
        matches = re.findall(pattern, profile_html, re.DOTALL | re.IGNORECASE)
        for match in matches:
            text = unescape(re.sub(r'<[^>]+>', ' ', match)).strip()
            text = re.sub(r'\s+', ' ', text)
            if len(text) > 50:
                return text
    all_paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', profile_html, re.DOTALL | re.IGNORECASE)
    for p in all_paragraphs:
        text = unescape(re.sub(r'<[^>]+>', ' ', p)).strip()
        text = re.sub(r'\s+', ' ', text)
        if len(text) > 100:
            return text
    return ""


def extract_website_from_profile(profile_html: str) -> str:
    website_links = re.findall(
        r'<a[^>]*target="_blank"[^>]*rel="nofollow"[^>]*href="([^"]*)"[^>]*>',
        profile_html, re.IGNORECASE
    )
    for href in website_links:
        href = href.strip()
        if is_plausible_website(href):
            return href
    button_links = re.findall(
        r'href="(https?://[^"]*)"[^>]*>.*?Website.*?</a>',
        profile_html, re.DOTALL | re.IGNORECASE
    )
    for href in button_links:
        if is_plausible_website(href):
            return href
    all_external = re.findall(
        r'href="(https?://(?!ensun\.io)[^"]*)"',
        profile_html, re.IGNORECASE
    )
    for href in all_external:
        href = href.strip()
        if is_plausible_website(href):
            return href
    return ""


def parse_company_profile(profile_html: str) -> tuple[str, str]:
    description = extract_company_description(profile_html)
    website = extract_website_from_profile(profile_html)
    if website and is_junk_website(website):
        website = ""
    return description, website


def fetch_page_requests(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=WEBSITE_TIMEOUT, allow_redirects=True)
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
        r'<div[^>]*class="[^"]*contact[^"]*"[^>]*>(.*?)</div>',
    ]
    for pattern in footer_patterns:
        match = re.search(pattern, text, re.I | re.DOTALL)
        if match:
            email = _find_email(match.group(1))
            if email:
                return email
    return _find_email(text)


def _find_email(text: str) -> Optional[str]:
    text = text.lower()
    mailto = re.search(r"mailto:([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})", text, re.I)
    if mailto:
        email = mailto.group(1)
        if is_useful_email(email):
            return email
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


# ===== SAVE FUNCTIONS (Matching Made-in-China format) =====

def to_deduped_dataframe(records: list[SupplierRecord]) -> pd.DataFrame:
    """Convert records to deduplicated DataFrame matching Made-in-China format."""
    df = pd.DataFrame([asdict(r) for r in records])
    if df.empty:
        return df
    # Ensure country is filled (like Made-in-China script)
    df["country"] = df["country"].fillna("").astype(str)
    # Normalize company names for deduplication
    df["company_name_norm"] = (
        df["company_name"].fillna("").astype(str).str.strip().str.lower().str.replace(r"\s+", " ", regex=True)
    )
    return df.drop_duplicates(subset=["company_name_norm"]).drop(columns=["company_name_norm"])


def save_scrape_checkpoint(records: list[SupplierRecord], output_path: str) -> None:
    """Save scraping progress - matches Made-in-China format."""
    df = to_deduped_dataframe(records)
    if not df.empty:
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"[SCRAPE SAVE] {len(df)} records → {output_path}")


def save_enrich_checkpoint(records: list[SupplierRecord], output_path: str) -> None:
    """Save enrichment progress - matches Made-in-China format."""
    df = to_deduped_dataframe(records)
    if not df.empty:
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"[ENRICH SAVE] {len(df)} records → {output_path}")


def filter_records_with_company_websites(records: list[SupplierRecord]) -> list[SupplierRecord]:
    """Keep only records with legitimate company websites."""
    kept = [r for r in records if r.website_url and is_plausible_website(r.website_url)]
    removed = len(records) - len(kept)
    print(f"[CLEANUP] Keeping {len(kept)} records with company websites; removed {removed}.")
    return kept


def enrich_emails_from_company_websites(records: list[SupplierRecord]) -> None:
    """Visit company websites to find emails - matches Made-in-China format."""
    if not ENABLE_WEBSITE_EMAIL_ENRICHMENT:
        return
    
    candidates = [r for r in records if r.website_url and not r.email]
    if MAX_WEBSITE_EMAIL_LOOKUPS > 0:
        candidates = candidates[:MAX_WEBSITE_EMAIL_LOOKUPS]
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
                print(f"  [{found}] {record.company_name[:40]} → {email}")
                
                if emails_since_checkpoint >= AUTOSAVE_EVERY_NEW_EMAILS:
                    save_enrich_checkpoint(records, PARTIAL_ENRICH_CSV)
                    print(f"  [ENRICH SAVE] {found} emails → {PARTIAL_ENRICH_CSV}")
                    emails_since_checkpoint = 0
            
            if i % 50 == 0:
                save_enrich_checkpoint(records, PARTIAL_ENRICH_CSV)
                print(f"  Progress: {i}/{len(candidates)}, found {found}, skipped {skipped}")
    
    save_enrich_checkpoint(records, PARTIAL_ENRICH_CSV)
    print(f"  [ENRICH DONE] {found} emails found, {skipped} timed out. Saved to {PARTIAL_ENRICH_CSV}")


def scrape_ensun():
    all_records = []
    seen_names = set()
    records_since_last_autosave = 0

    print("=" * 60)
    print("Ensun.io Cosmetic Packaging Supplier Scraper")
    print(f"Target: {TARGET_SUPPLIERS} suppliers")
    print(f"Permanent Filters: {PERMANENT_FILTERS}")
    print(f"Email timeout: {WEBSITE_TIMEOUT}s | Contact paths: {len(CONTACT_PATHS)}")
    print("=" * 60)

    try:
        with EnsunBrowser() as browser:
            if not browser.enabled:
                print("[ERROR] Playwright is required for ensun.io!")
                return pd.DataFrame()
            
            for keyword in KEYWORDS:
                if len(all_records) >= TARGET_SUPPLIERS:
                    break
                print(f"\n[KEYWORD] {keyword}")
                for country in COUNTRIES:
                    if len(all_records) >= TARGET_SUPPLIERS:
                        break
                    zero_data_pages = 0
                    for page in range(1, MAX_PAGES_PER_QUERY + 1):
                        if len(all_records) >= TARGET_SUPPLIERS:
                            break

                        url = build_search_url(keyword, country, page)
                        status, html = browser.fetch_search_page(url)

                        if status != 200 or not html:
                            zero_data_pages += 1
                            if zero_data_pages >= 1:
                                print(f"  [{keyword}] [{country}] Empty page; moving to next combination.")
                                break
                            random_delay()
                            continue

                        if 'Vercel Security Checkpoint' in html or '429' in html:
                            print(f"  [{keyword}] [{country}] Blocked by Vercel! Waiting 60s...")
                            time.sleep(60)
                            zero_data_pages += 1
                            if zero_data_pages >= 1:
                                break
                            continue

                        companies = browser.extract_company_cards()
                        if not companies:
                            print(f"  [{keyword}] [{country}] Waiting for company cards...")
                            browser._wait_for_company_cards()
                            companies = browser.extract_company_cards()
                        if not companies or any(not c.get("profile_url") for c in companies):
                            html_companies = extract_company_links_from_search(html)
                            if html_companies:
                                by_name = {
                                    re.sub(r"\s+", " ", c["name"]).strip().lower(): c
                                    for c in html_companies
                                }
                                for company in companies:
                                    norm = re.sub(r"\s+", " ", company["name"]).strip().lower()
                                    if not company.get("profile_url") and norm in by_name:
                                        company["profile_url"] = normalize_profile_url(by_name[norm].get("profile_url", ""))
                                if not companies:
                                    companies = html_companies
                        
                        if not companies:
                            zero_data_pages += 1
                            if zero_data_pages >= 1:
                                print(f"  [{keyword}] [{country}] No results; moving to next combination.")
                                break
                            random_delay()
                            continue

                        page_new = 0
                        for card_index, company in enumerate(companies):
                            norm = re.sub(r"\s+", " ", company["name"]).strip().lower()
                            if not norm or norm in seen_names:
                                continue
                            seen_names.add(norm)
                            
                            description = ""
                            website = ""
                            profile_url = normalize_profile_url(company["profile_url"])
                            if FETCH_PROFILE_DETAILS_IN_PHASE1:
                                if profile_url:
                                    status, profile_html = browser.fetch_profile_page(profile_url)
                                else:
                                    status, profile_url, profile_html = browser.click_company_card_by_index(
                                        card_index,
                                        company["name"],
                                    )
                                    if status != 200:
                                        status, profile_url, profile_html = browser.click_company_card_for_profile(
                                            url,
                                            company["name"],
                                        )
                                if status == 200 and profile_html:
                                    description, website = parse_company_profile(profile_html)
                                browser.return_to_listing(url)
                            
                            record = SupplierRecord(
                                company_name=company["name"],
                                profile_url=profile_url,
                                country=country,
                                website_url=website,
                                company_description=description,
                            )
                            
                            all_records.append(record)
                            page_new += 1
                            records_since_last_autosave += 1
                            
                            if website:
                                print(f"    [{page_new}] {company['name'][:50]} → {website}")
                            else:
                                print(f"    [{page_new}] {company['name'][:50]}")
                            
                            if records_since_last_autosave >= AUTOSAVE_EVERY_NEW_RECORDS:
                                save_scrape_checkpoint(all_records, PARTIAL_SCRAPE_CSV)
                                print(f"[SCRAPE SAVE] {len(all_records)} records → {PARTIAL_SCRAPE_CSV}")
                                records_since_last_autosave = 0
                            
                            random_delay()

                        if page_new:
                            zero_data_pages = 0
                        print(f"  [{keyword}] [{country}] Page {page}: +{page_new} (Total: {len(all_records)})")
                        random_delay()

    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Saving progress...")
        save_scrape_checkpoint(all_records, PARTIAL_SCRAPE_CSV)
        print(f"[SAVED] {len(all_records)} records to {PARTIAL_SCRAPE_CSV}")
        return to_deduped_dataframe(all_records)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        save_scrape_checkpoint(all_records, PARTIAL_SCRAPE_CSV)

    save_scrape_checkpoint(all_records, PARTIAL_SCRAPE_CSV)
    print(f"\n[PHASE 1 DONE] {len(all_records)} companies scraped with profile details. Saved to {PARTIAL_SCRAPE_CSV}")

    # Drop records without websites before email enrichment; emails require a company website.
    if all_records and FILTER_RECORDS_WITH_WEBSITES:
        all_records = filter_records_with_company_websites(all_records)
        save_enrich_checkpoint(all_records, PARTIAL_ENRICH_CSV)

    # Phase 2: email enrichment from websites collected in phase 1.
    if all_records and ENABLE_WEBSITE_EMAIL_ENRICHMENT:
        print("\n[PHASE 2] Starting email enrichment for companies with websites...")
        enrich_emails_from_company_websites(all_records)
        save_enrich_checkpoint(all_records, PARTIAL_ENRICH_CSV)

    return to_deduped_dataframe(all_records)


def main() -> None:
    started_at = datetime.now()
    print(f"[RUN] Started at: {started_at.strftime('%Y-%m-%d %H:%M:%S')}")

    df = scrape_ensun()
    ended_at = datetime.now()
    elapsed_minutes = (ended_at - started_at).total_seconds() / 60

    if df.empty:
        print("[INFO] No records found.")
        print(f"[RUN] Started at: {started_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[RUN] Ended at:   {ended_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[RUN] Total time: {elapsed_minutes:.2f} minutes")
        return
    
    # Final output matching Made-in-China format
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
    print(f"  Started at: {started_at.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Ended at:   {ended_at.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Total time: {elapsed_minutes:.2f} minutes")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
