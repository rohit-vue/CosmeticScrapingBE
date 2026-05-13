"""
Europages supplier scraper - Complete with all phases.
Phase 1: Scrape company profiles with lazy-load scrolling
Phase 2: Contact page enrichment (website extraction with scrolling)
Phase 3: Email enrichment from company websites
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
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from ai_supplier_filter import apply_ai_filter_to_records
from scraper_runtime_config import env_int, env_list

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except ImportError:
    PlaywrightTimeoutError = TimeoutError
    sync_playwright = None
    print("⚠ Playwright not installed. Run: pip install playwright && playwright install")

# ===== CONFIGURATION =====
SOURCE_DIRECTORY = "Europages"
BASE_DOMAIN = "https://www.europages.co.uk"
MAX_PAGES_PER_KEYWORD = 50
ZERO_NEW_PAGES_CUTOFF = 3
MIN_DELAY_SECONDS = 5
MAX_DELAY_SECONDS = 8
OUTPUT_CSV = "europages_suppliers_phase1_raw.csv"
CLEANED_CSV = "europages_suppliers_cleaned.csv"
PARTIAL_SCRAPE_CSV = "europages_suppliers_scrape_progress.csv"
PARTIAL_ENRICH_CSV = "europages_suppliers_enrich_progress.csv"
TARGET_SUPPLIERS = 5000
AUTOSAVE_EVERY_NEW_RECORDS = 10
AUTOSAVE_EVERY_NEW_EMAILS = 10

ENABLE_CONTACT_PAGE_ENRICHMENT = True
MAX_CONTACT_ENRICHMENTS = TARGET_SUPPLIERS
REFRESH_WEBSITE_URLS = os.getenv("REFRESH_WEBSITE_URLS", "1").strip().lower() not in {"0", "false", "no"}

ENABLE_WEBSITE_EMAIL_ENRICHMENT = True
MAX_WEBSITE_EMAIL_LOOKUPS = TARGET_SUPPLIERS
WEBSITE_EMAIL_WORKERS = 10
WEBSITE_TIMEOUT = 15
ENABLE_AI_FILTERING = True
AI_VERIFIED_CSV = "europages_ai_verified.csv"
AI_REJECTED_CSV = "europages_ai_rejected.csv"
AI_CHECKPOINT_CSV = "europages_ai_checkpoint.csv"
AI_BATCH_SIZE = int(os.getenv("AI_BATCH_SIZE", "20"))
AI_CONCURRENT = int(os.getenv("AI_CONCURRENT", "3"))

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
    "/impressum",
    "/imprint",
    "/legal-notice",
    "/kontakt",
    "/uber-uns",
]

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

COUNTRIES = {
    "Russia": "RU",
    "Ukraine": "UA",
    "Poland": "PL",
    "Czech Republic": "CZ",
    "Hungary": "HU",
    "Romania": "RO",
    "Bulgaria": "BG",
    "Belarus": "BY",
    "Serbia": "RS",
    "Croatia": "HR",
    "Slovakia": "SK",
    "Slovenia": "SI",
    "Lithuania": "LT",
    "Latvia": "LV",
    "Turkey": "TR",
}

KEYWORDS = env_list("SCRAPER_KEYWORDS", KEYWORDS)
_DEFAULT_COUNTRIES = COUNTRIES
_REQUESTED_COUNTRIES = env_list("SCRAPER_COUNTRIES", list(_DEFAULT_COUNTRIES.keys()))
COUNTRIES = {k: v for k, v in _DEFAULT_COUNTRIES.items() if k in _REQUESTED_COUNTRIES} or _DEFAULT_COUNTRIES
TARGET_SUPPLIERS = env_int("SCRAPER_TARGET_SUPPLIERS", TARGET_SUPPLIERS)

EUROPAGES_PROFILE_DIR = ".europages_playwright_profile"
BROWSER_TIMEOUT_MS = 120000
PLAYWRIGHT_HEADLESS = False

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

JUNK_NAME_PATTERNS = [
    r'view portfolio', r'portfolio\s*\(\d+\)',
    r'contact supplier', r'send message', r'request quote',
    r'see details', r'learn more', r'^\d+$', r'^\.+$',
]

JUNK_EMAIL_PHRASES = [
    "cloudflare", "404", "notfound", "blocked", "error",
    "ordercreditreport", "copyright", "@anytime", "@theforefront",
    "@homeandabroad", "@thistime", "@www", "pleasefeel"
]

COOKIE_BUTTONS = [
    "Accept All", "Accept all", "Accept", "OK", "Agree",
    "Allow All", "Got it", "Reject All", "Only Necessary",
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


def build_search_url(keyword: str, country_code: str, page: int) -> str:
    encoded_keyword = requests.utils.quote(keyword)
    return f"{BASE_DOMAIN}/en/search/page/{page}?countries={country_code}&q={encoded_keyword}"


def is_junk_name(name: str) -> bool:
    name_lower = name.lower().strip()
    for pattern in JUNK_NAME_PATTERNS:
        if re.search(pattern, name_lower, re.I):
            return True
    return len(name) < 3


def save_records(records: list[SupplierRecord], filepath: str):
    if records:
        df = pd.DataFrame([asdict(r) for r in records])
        df.to_csv(filepath, index=False, encoding="utf-8-sig", sep='\t')
        print(f"  💾 Saved {len(records)} records to {filepath}")


def strip_tags(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", unescape(text))).strip()


def extract_company_description_from_profile(html: str) -> str:
    patterns = [
        r'<p[^>]*data-test=["\']company-description-full["\'][^>]*>(.*?)</p>',
        r'<[^>]+data-test=["\']company-description-full["\'][^>]*>(.*?)</[^>]+>',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, re.I | re.S)
        if match:
            text = strip_tags(match.group(1))
            if text and len(text) > 30:
                return text
    return ""


def is_plausible_website(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.netloc or "").lower()
    if not host or any(x in host for x in ["europages.com", "europages.co.uk"]):
        return False
    blocked = ("facebook.com", "instagram.com", "linkedin.com", "youtube.com", "wa.me")
    return not any(t in host for t in blocked)


def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if url.startswith(("http://", "https://")):
        return url
    if url.startswith("www."):
        return f"https://{url}"
    if "." in url and " " not in url:
        return f"https://{url}"
    return ""


def normalize_landed_website_url(url: str) -> str:
    """Keep the final browser URL, minus fragments added by client-side routing."""
    url = normalize_url(url)
    if not is_plausible_website(url):
        return ""
    parsed = urlparse(url)
    return parsed._replace(fragment="").geturl()


def clean_email(email: str) -> Optional[str]:
    if not email:
        return None
    match = re.match(r'([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})([A-Z].*)?', email)
    return match.group(1) if match else email


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
            email = _find_email_in_text(match.group(1))
            if email:
                return email
    
    return _find_email_in_text(text)


def _find_email_in_text(text: str) -> Optional[str]:
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
    if match:
        email = match.group(0)
        if is_useful_email(email):
            return email
    return None


def is_useful_email(email: str) -> bool:
    if not email or len(email) > 50 or len(email) < 6:
        return False
    if ' ' in email or email.count('@') != 1:
        return False
    if not re.search(r'\.(com|net|org|de|fr|it|es|uk|nl|be|at|ch|pl|eu|ru|ua|cz|hu|ro|bg|by|rs|hr|sk|si|lt|lv|tr|co\.\w+)$', email):
        return False
    blocked = ["europages.com", "europages.co.uk", "example.com"]
    if any(b in email.lower() for b in blocked):
        return False
    if any(j in email.lower() for j in JUNK_EMAIL_PHRASES):
        return False
    return True


def fetch_page_requests(url: str, timeout: int = WEBSITE_TIMEOUT) -> Optional[str]:
    try:
        resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
        return None
    except Exception:
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


class EuropagesScraper:
    def __init__(self):
        self.playwright = None
        self.context = None
        self.page = None
        self.captcha_solved = False
    
    def _is_browser_alive(self) -> bool:
        try:
            if not self.page:
                return False
            self.page.url
            return True
        except:
            return False
    
    def _launch_browser(self):
        if self.playwright:
            try:
                self.playwright.stop()
            except:
                pass
        
        print("  🌐 Launching browser...")
        self.playwright = sync_playwright().start()
        
        user_data = str(Path(EUROPAGES_PROFILE_DIR).resolve())
        
        self.context = self.playwright.chromium.launch_persistent_context(
            user_data_dir=user_data,
            headless=PLAYWRIGHT_HEADLESS,
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="Europe/London",
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        
        self.page = self.context.new_page()
        self.page.set_default_navigation_timeout(BROWSER_TIMEOUT_MS)
        print("  📦 Browser ready\n")
    
    def __enter__(self):
        if sync_playwright is None:
            raise RuntimeError("Playwright is not installed")
        self._launch_browser()
        return self
    
    def __exit__(self, *args):
        try:
            if self.context:
                self.context.close()
        except:
            pass
        try:
            if self.playwright:
                self.playwright.stop()
        except:
            pass
    
    def dismiss_cookies(self):
        if not self._is_browser_alive():
            return
        try:
            self.page.wait_for_timeout(2000)
            for text in COOKIE_BUTTONS:
                try:
                    btn = self.page.query_selector(f'button:has-text("{text}")')
                    if btn and btn.is_visible():
                        btn.click()
                        self.page.wait_for_timeout(500)
                        return
                except:
                    pass
            self.page.keyboard.press('Escape')
            self.page.wait_for_timeout(500)
        except:
            pass
    
    def _has_captcha_elements(self) -> bool:
        if not self._is_browser_alive():
            return False
        try:
            begin_btn = self.page.query_selector('button:has-text("Begin")')
            if begin_btn and begin_btn.is_visible():
                return True
            body_text = self.page.inner_text('body').lower()
            captcha_keywords = [
                'confirm you are human', 'complete the security check',
                'choose all the', 'select all images', 'verify you are human',
            ]
            for kw in captcha_keywords:
                if kw in body_text:
                    return True
            return False
        except:
            return False
    
    def wait_for_captcha_solve(self) -> bool:
        print("\n  ╔══════════════════════════════════════════════════╗")
        print("  ║  🔐 CAPTCHA! Solve it in the browser window.     ║")
        print("  ╚══════════════════════════════════════════════════╝\n")
        
        max_wait = 300
        start = time.time()
        last_msg = 0
        
        while time.time() - start < max_wait:
            time.sleep(3)
            
            if not self._is_browser_alive():
                print("  ⚠ Browser closed! Restarting...")
                self._launch_browser()
                return False
            
            if not self._has_captcha_elements():
                self.page.wait_for_timeout(2000)
                body_text = self.page.inner_text('body')
                if len(body_text) > 100:
                    elapsed = int(time.time() - start)
                    print(f"  ✅ Solved! ({elapsed}s)\n")
                    self.captcha_solved = True
                    return True
            
            elapsed = int(time.time() - start)
            if elapsed - last_msg >= 30:
                print(f"  ⏳ Waiting... ({elapsed}s)")
                last_msg = elapsed
        
        print("  ⚠ Timeout\n")
        return False
    
    def navigate_to_page(self, url: str, page_num: int) -> Optional[str]:
        if not self._is_browser_alive():
            print("  ⚠ Browser not running. Restarting...")
            self._launch_browser()
            self.captcha_solved = False
        
        try:
            print(f"  📄 Page {page_num}")
            self.page.goto(url, wait_until="domcontentloaded")
            self.page.wait_for_timeout(3000)
            self.dismiss_cookies()
            
            if self._has_captcha_elements():
                if not self.wait_for_captcha_solve():
                    return None
            
            print("  ⏳ Rendering...")
            self.page.wait_for_timeout(3000)
            self.page.evaluate("window.scrollTo(0, 500)")
            self.page.wait_for_timeout(1000)
            self.page.evaluate("window.scrollTo(0, 0)")
            self.page.wait_for_timeout(1000)
            
            return self.page.content()
        except Exception as e:
            print(f"  ❌ Error: {e}")
            return None
    
    def navigate_and_scroll_profile(self, url: str) -> Optional[str]:
        """Navigate to profile page and scroll to load lazy content. Returns full HTML."""
        if not self._is_browser_alive():
            self._launch_browser()
        
        try:
            self.page.goto(url, wait_until="domcontentloaded")
            self.page.wait_for_timeout(3000)
            self.dismiss_cookies()
            
            # Scroll down in steps to trigger lazy loading of contact/website section
            for scroll_pos in [300, 600, 900, 1200]:
                self.page.evaluate(f"window.scrollTo(0, {scroll_pos})")
                self.page.wait_for_timeout(500)
            
            # Scroll back to middle where website link usually is
            self.page.evaluate("window.scrollTo(0, 600)")
            self.page.wait_for_timeout(1000)
            
            return self.page.content()
        except Exception as e:
            print(f"    ⚠ Error scrolling profile: {e}")
            return None

    def _wait_for_landed_website_url(self, target_page) -> str:
        """Wait for Europages' outbound redirect to finish and return the real site URL."""
        deadline = time.time() + 20
        best_url = ""
        
        while time.time() < deadline:
            try:
                target_page.wait_for_load_state("domcontentloaded", timeout=3000)
            except Exception:
                pass
            
            try:
                current_url = normalize_landed_website_url(target_page.url)
            except Exception:
                current_url = ""
            
            if current_url:
                best_url = current_url
                try:
                    target_page.wait_for_load_state("networkidle", timeout=3000)
                    settled_url = normalize_landed_website_url(target_page.url)
                    if settled_url:
                        return settled_url
                except Exception:
                    return best_url
            
            try:
                target_page.wait_for_timeout(1000)
            except Exception:
                break
        
        return best_url

    def click_company_website_from_current_profile(self) -> str:
        """Click the visible Company's Website link and capture the final landed URL."""
        if not self._is_browser_alive():
            return ""
        
        website_link_selectors = [
            'xpath=//*[contains(@class, "font-copy-400") and contains(normalize-space(.), "Company") and contains(normalize-space(.), "Website")]/ancestor::a[1]',
            'xpath=//a[contains(normalize-space(.), "Company") and contains(normalize-space(.), "Website")]',
            'xpath=//a[contains(normalize-space(.), "Visit website") or contains(normalize-space(.), "Visit Website")]',
        ]
        
        for selector in website_link_selectors:
            try:
                link = self.page.locator(selector).first
                if link.count() == 0:
                    continue
                
                link.scroll_into_view_if_needed(timeout=5000)
                self.page.wait_for_timeout(500)
                
                target_page = None
                try:
                    with self.page.expect_popup(timeout=5000) as popup_info:
                        link.click(timeout=7000)
                    target_page = popup_info.value
                except PlaywrightTimeoutError:
                    target_page = self.page
                except Exception:
                    try:
                        link.click(timeout=7000)
                        target_page = self.page
                    except Exception:
                        continue
                
                website_url = self._wait_for_landed_website_url(target_page)
                
                if target_page != self.page:
                    try:
                        target_page.close()
                    except Exception:
                        pass
                
                if website_url:
                    return website_url
            except Exception:
                continue
        
        return ""

    def extract_website_from_current_page_dom(self) -> str:
        """Read the visible Company's Website link from the rendered profile page."""
        if not self._is_browser_alive():
            return ""

        try:
            website_url = self.page.evaluate("""
                () => {
                    const blocked = [
                        'europages.com',
                        'europages.co.uk',
                        'facebook.com',
                        'instagram.com',
                        'linkedin.com',
                        'youtube.com',
                        'wa.me'
                    ];

                    const isExternal = (href) => {
                        if (!href || !/^https?:\\/\\//i.test(href)) return false;
                        const lower = href.toLowerCase();
                        return !blocked.some(domain => lower.includes(domain));
                    };

                    const anchors = Array.from(document.querySelectorAll('a[href]'));
                    for (const anchor of anchors) {
                        const text = (anchor.innerText || anchor.textContent || '').trim().toLowerCase();
                        const label = [
                            text,
                            anchor.getAttribute('aria-label') || '',
                            anchor.getAttribute('title') || '',
                            anchor.getAttribute('data-test') || '',
                            anchor.getAttribute('href') || ''
                        ].join(' ').toLowerCase();
                        const href = anchor.href || anchor.getAttribute('href') || '';

                        if (
                            isExternal(href) &&
                            (
                                label.includes("company's website") ||
                                label.includes('company website') ||
                                label.includes('visit website') ||
                                text === 'website' ||
                                text.includes('website')
                            )
                        ) {
                            return href;
                        }
                    }

                    return '';
                }
            """)
            website_url = normalize_landed_website_url(website_url)
            if website_url:
                return website_url
        except Exception:
            pass

        return ""

    def extract_description_from_current_page(self) -> str:
        try:
            locator = self.page.locator('[data-test="company-description-full"]').first
            if locator.count() > 0:
                text = locator.inner_text(timeout=2500)
                text = re.sub(r"\s+", " ", text or "").strip()
                if len(text) > 30:
                    return text
        except Exception:
            pass
        return ""
    
    def parse_company_listings(self, html: str) -> list[dict]:
        companies = []
        seen_urls = set()
        
        profile_patterns = [
            r'href="(/[^"]+/\d+-\d+\.html)"',
            r'href="(/[^"]*?/\d+-\d+\.[^"]+)"',
        ]
        
        profile_links = []
        for pattern in profile_patterns:
            links = list(set(re.findall(pattern, html)))
            if links:
                profile_links = links
                break
        
        if not profile_links:
            return companies
        
        print(f"  🔗 {len(profile_links)} profile links")
        
        for link in profile_links:
            if link in seen_urls:
                continue
            
            pos = html.find(link)
            if pos == -1:
                continue
            
            context = html[max(0, pos-500):min(len(html), pos+1500)]
            
            name = None
            h2_match = re.search(r'<h2[^>]*>(.*?)</h2>', context, re.I | re.DOTALL)
            if h2_match:
                name = re.sub(r'<[^>]+>', ' ', h2_match.group(1)).strip()
                name = unescape(name)
            
            if not name:
                link_match = re.search(
                    r'<a[^>]*href="' + re.escape(link) + r'"[^>]*>(.*?)</a>',
                    context, re.I | re.DOTALL
                )
                if link_match:
                    name = re.sub(r'<[^>]+>', ' ', link_match.group(1)).strip()
                    name = unescape(name)
            
            if name:
                name = re.sub(r'\s+', ' ', name).strip()
                name = re.sub(r'&amp;', '&', name)
            
            if not name or is_junk_name(name):
                continue
            
            profile_url = urljoin(BASE_DOMAIN, link)
            seen_urls.add(link)
            companies.append({"name": name, "profile_url": profile_url})
        
        return companies
    
    def extract_website_from_profile(self, html: str) -> str:
        """Extract company website from profile page HTML after scrolling."""
        patterns = [
            # Company's Website label near a link
            r'Company\'?s?\s*Website.*?href="(https?://[^"]+)"',
            # Link with text-primary class
            r'<a[^>]*href="(https?://(?!.*europages)[^"]+)"[^>]*class="[^"]*text-primary[^"]*"[^>]*>',
            # Generic website patterns
            r'<a[^>]*href="(https?://(?!.*europages)[^"]+)"[^>]*>\s*(?:Website|Visit website|www\.|Company\'?s?\s*Website)\s*</a>',
            r'<a[^>]*href="(https?://(?!.*europages)[^"]+)"[^>]*class="[^"]*website[^"]*"[^>]*>',
            r'Website[:\s]*<a[^>]+href="(https?://[^"]+)"',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, html, re.I | re.DOTALL)
            if match:
                url = normalize_url(match.group(1))
                if is_plausible_website(url):
                    return url
        
        # Fallback: Find "Company's Website" text and look for any href nearby
        for search_text in ["Company's Website", "company website", "Website"]:
            pos = html.find(search_text)
            if pos != -1:
                context = html[max(0, pos-500):min(len(html), pos+500)]
                link_match = re.search(r'href="(https?://[^"]+)"', context, re.I)
                if link_match:
                    url = normalize_url(link_match.group(1))
                    if is_plausible_website(url):
                        return url
        
        return ""


def enrich_from_profile_page(scraper: EuropagesScraper, record: SupplierRecord) -> None:
    """Visit company profile, scroll to load content, extract website and email."""
    if not REFRESH_WEBSITE_URLS and record.website_url and record.email and record.company_description:
        return
    
    try:
        # Use the new scroll method
        html = scraper.navigate_and_scroll_profile(record.profile_url)
        
        if html:
            if not record.company_description:
                description = scraper.extract_description_from_current_page()
                if not description:
                    description = extract_company_description_from_profile(html)
                if description:
                    record.company_description = description
            # Click through Europages' website link and keep the final landed URL.
            if REFRESH_WEBSITE_URLS or not record.website_url:
                old_website = record.website_url
                website = scraper.extract_website_from_current_page_dom()
                if not website:
                    website = scraper.extract_website_from_profile(html)
                if not website:
                    website = scraper.click_company_website_from_current_profile()
                if website:
                    if record.email and (not old_website or old_website != website):
                        record.email = ""
                        print("    [email] cleared old email because website changed")
                    record.website_url = website
                    print(f"    🌐 {website}")
            
            # Only trust profile emails when no real website is available.
            # If a real website was captured, Phase 3 will extract email from that domain.
            if not record.email and not record.website_url:
                email = extract_email_from_html(html)
                email = clean_email(email)
                if email and is_useful_email(email):
                    record.email = email
                    print(f"    📧 {email}")
                    
    except Exception as e:
        print(f"    ⚠ Error: {e}")


def run_contact_page_enrichment(records: list[SupplierRecord]) -> None:
    """Phase 2: Enrich records by visiting company profiles with scrolling."""
    if not ENABLE_CONTACT_PAGE_ENRICHMENT:
        return
    
    candidates = [
        r for r in records
        if REFRESH_WEBSITE_URLS or not r.website_url or not r.email or not r.company_description
    ]
    if not candidates:
        print("\n[PHASE 2] All records already have websites/emails. Skipping.")
        return
    
    print(f"\n{'='*60}")
    print(f"[PHASE 2] Contact Page Enrichment")
    print(f"  Visiting {min(len(candidates), MAX_CONTACT_ENRICHMENTS)} profiles...")
    print(f"  (Clicks Company's Website and stores the final landed URL)")
    print(f"{'='*60}")
    
    with EuropagesScraper() as scraper:
        attempted = 0
        websites_found = 0
        emails_found = 0
        new_since_checkpoint = 0
        
        for record in candidates:
            if attempted >= MAX_CONTACT_ENRICHMENTS:
                print(f"\n  Reached cap of {MAX_CONTACT_ENRICHMENTS}. Stopping.")
                break
            
            before_website = record.website_url
            before_email = record.email
            
            print(f"\n  [{attempted+1}/{min(len(candidates), MAX_CONTACT_ENRICHMENTS)}] {record.company_name[:60]}")
            
            try:
                enrich_from_profile_page(scraper, record)
            except Exception as e:
                print(f"    ⚠ Error: {e}")
            
            attempted += 1
            
            if record.website_url != before_website:
                websites_found += 1
                new_since_checkpoint += 1
            
            if record.email and record.email != before_email:
                emails_found += 1
            
            if new_since_checkpoint >= AUTOSAVE_EVERY_NEW_RECORDS:
                save_records(records, PARTIAL_SCRAPE_CSV)
                print(f"  💾 Saved ({websites_found} websites, {emails_found} emails)")
                new_since_checkpoint = 0
            
            if attempted % 10 == 0:
                print(f"\n  📊 {attempted} profiles | {websites_found} websites | {emails_found} emails")
            
            time.sleep(random.uniform(3, 5))
    
    save_records(records, PARTIAL_SCRAPE_CSV)
    print(f"\n[PHASE 2 DONE] {attempted} visited | {websites_found} websites | {emails_found} emails")


def enrich_emails_from_websites(records: list[SupplierRecord]) -> None:
    """Phase 3: Extract emails from company websites."""
    if not ENABLE_WEBSITE_EMAIL_ENRICHMENT:
        return
    
    candidates = [r for r in records if r.website_url and not r.email][:MAX_WEBSITE_EMAIL_LOOKUPS]
    
    if not candidates:
        print("\n[PHASE 3] No companies need email enrichment. Skipping.")
        return
    
    print(f"\n{'='*60}")
    print(f"[PHASE 3] Email Enrichment from Company Websites")
    print(f"  Checking {len(candidates)} websites...")
    print(f"  Strategy: {len(CONTACT_PATHS)} contact page variations | Timeout: {WEBSITE_TIMEOUT}s")
    print(f"{'='*60}")
    
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
                print(f"  📧 [{found}] {record.company_name[:50]} → {email}")
                
                if emails_since_checkpoint >= AUTOSAVE_EVERY_NEW_EMAILS:
                    save_records(records, PARTIAL_ENRICH_CSV)
                    print(f"  💾 Saved {found} emails → {PARTIAL_ENRICH_CSV}")
                    emails_since_checkpoint = 0
            
            if i % 50 == 0:
                print(f"  Progress: {i}/{len(candidates)}, found {found}, skipped {skipped}")
    
    save_records(records, PARTIAL_ENRICH_CSV)
    print(f"\n[PHASE 3 DONE] {found} emails found, {skipped} timed out")


def main():
    print("=" * 60)
    print("Europages Supplier Scraper - All Phases")
    print(f"Target: {TARGET_SUPPLIERS} | Keywords: {len(KEYWORDS)} | Countries: {len(COUNTRIES)}")
    print("=" * 60)
    print(f"\n  Phase 1: Scrape company listings")
    print(f"  Phase 2: Visit profiles for websites (with scroll)")
    print(f"  Phase 3: Extract emails from websites")
    print(f"\n  📌 Solve CAPTCHA once in the browser window.\n")
    
    input("Press ENTER to start...")
    
    all_records = []
    seen_names = set()
    last_save_count = 0
    
    # ================================================================
    # PHASE 1: Scrape search results
    # ================================================================
    print(f"\n{'='*60}")
    print(f"PHASE 1: Scraping Company Listings")
    print(f"{'='*60}")
    
    with EuropagesScraper() as scraper:
        for keyword in KEYWORDS:
            if len(all_records) >= TARGET_SUPPLIERS:
                print(f"\n  ✅ Target reached: {TARGET_SUPPLIERS} suppliers")
                break
            
            for country_name, country_code in COUNTRIES.items():
                if len(all_records) >= TARGET_SUPPLIERS:
                    break
                
                print(f"\n{'='*60}")
                print(f"🔍 '{keyword}' | {country_name}")
                print(f"{'='*60}")
                
                consecutive_empty = 0
                
                for page in range(1, MAX_PAGES_PER_KEYWORD + 1):
                    if len(all_records) >= TARGET_SUPPLIERS:
                        break
                    
                    if consecutive_empty >= ZERO_NEW_PAGES_CUTOFF:
                        print(f"  No results for {ZERO_NEW_PAGES_CUTOFF} pages. Moving on.")
                        save_records(all_records, PARTIAL_SCRAPE_CSV)
                        last_save_count = len(all_records)
                        break
                    
                    url = build_search_url(keyword, country_code, page)
                    html = scraper.navigate_to_page(url, page)
                    
                    if not html:
                        consecutive_empty += 1
                        continue
                    
                    companies = scraper.parse_company_listings(html)
                    
                    new_count = 0
                    for c in companies:
                        norm = c['name'].lower().strip()
                        if norm not in seen_names:
                            seen_names.add(norm)
                            all_records.append(SupplierRecord(
                                company_name=c['name'],
                                country=country_name,
                                profile_url=c['profile_url'],
                            ))
                            new_count += 1
                    
                    if new_count == 0:
                        print(f"  📭 0 new")
                        consecutive_empty += 1
                    else:
                        consecutive_empty = 0
                        print(f"  ✅ +{new_count} (Total: {len(all_records)})")
                        for c in companies[:3]:
                            print(f"     • {c['name'][:70]}")
                        
                        if len(all_records) - last_save_count >= AUTOSAVE_EVERY_NEW_RECORDS:
                            save_records(all_records, PARTIAL_SCRAPE_CSV)
                            last_save_count = len(all_records)
                    
                    time.sleep(random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))
            
            if len(all_records) > last_save_count:
                save_records(all_records, PARTIAL_SCRAPE_CSV)
                last_save_count = len(all_records)
    
    save_records(all_records, PARTIAL_SCRAPE_CSV)
    
    print(f"\n{'='*60}")
    print(f"[PHASE 1 DONE] {len(all_records)} companies scraped")
    print(f"  Progress: {PARTIAL_SCRAPE_CSV}")
    print(f"{'='*60}")
    
    # ================================================================
    # PHASE 2: Contact page enrichment (with scrolling)
    # ================================================================
    if all_records and ENABLE_CONTACT_PAGE_ENRICHMENT:
        run_contact_page_enrichment(all_records)
    
    # ================================================================
    # PHASE 3: AI filtering
    # ================================================================
    if all_records and ENABLE_AI_FILTERING:
        all_records = apply_ai_filter_to_records(
            all_records,
            keywords=KEYWORDS,
            source_name=SOURCE_DIRECTORY,
            verified_csv=AI_VERIFIED_CSV,
            rejected_csv=AI_REJECTED_CSV,
            checkpoint_csv=AI_CHECKPOINT_CSV,
            batch_size=AI_BATCH_SIZE,
            concurrent=AI_CONCURRENT,
        )
        save_records(all_records, PARTIAL_ENRICH_CSV)
    
    # ================================================================
    # PHASE 4: Email enrichment from websites
    # ================================================================
    if all_records and ENABLE_WEBSITE_EMAIL_ENRICHMENT:
        enrich_emails_from_websites(all_records)
    
    # ================================================================
    # FINAL OUTPUT
    # ================================================================
    if all_records:
        df = pd.DataFrame([asdict(r) for r in all_records])
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig", sep='\t')
        
        df_clean = df[df['email'].notna() & (df['email'] != '')]
        df_clean.to_csv(CLEANED_CSV, index=False, encoding="utf-8-sig", sep='\t')
        
        with_website = (df['website_url'] != '').sum()
        with_email = (df['email'] != '').sum()
    
    print(f"\n{'='*60}")
    print(f"✅ ALL PHASES COMPLETE!")
    print(f"  Total suppliers:     {len(df)}")
    print(f"  With website:        {with_website}")
    print(f"  With email:          {with_email}")
    print(f"  Phase 1 progress:    {PARTIAL_SCRAPE_CSV}")
    print(f"  Phase 3 progress:    {PARTIAL_ENRICH_CSV}")
    print(f"  Raw output:          {OUTPUT_CSV}")
    print(f"  Cleaned (w/ email):  {CLEANED_CSV} ({len(df_clean)} suppliers)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
