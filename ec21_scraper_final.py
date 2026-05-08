"""
EC21 supplier scraper - Simplified with better contact page handling.
Uses requests for email enrichment with proper timeout control.
"""

from __future__ import annotations

import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from dataclasses import dataclass, asdict
from html import unescape
from typing import Optional
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from scrapling import StealthyFetcher as ScraplingStealthFetcher

# ===== CONFIGURATION =====
SOURCE_DIRECTORY = "EC21"
BASE_DOMAIN = "https://www.ec21.com"
MAX_PAGES_PER_CATEGORY = 50
ZERO_NEW_PAGES_CUTOFF = 3
MIN_DELAY_SECONDS = 1
MAX_DELAY_SECONDS = 3
OUTPUT_CSV = "ec21_suppliers_phase1_raw.csv"
CLEANED_CSV = "ec21_suppliers_cleaned.csv"
PARTIAL_SCRAPE_CSV = "ec21_suppliers_scrape_progress.csv"
PARTIAL_ENRICH_CSV = "ec21_suppliers_enrich_progress.csv"
TARGET_SUPPLIERS = 2000
AUTOSAVE_EVERY_NEW_RECORDS = 5
AUTOSAVE_EVERY_NEW_EMAILS = 5
PROFILE_WORKERS = 3
WEBSITE_EMAIL_WORKERS = 5
WEBSITE_TIMEOUT = 15 # seconds per page

# Contact pages to try (in order - stops at first success)
CONTACT_PATHS = [
    "",                    # homepage (footer often here)
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

CATEGORIES = [
    "cosmetics",
    "cosmetic-packaging",
    "cosmetic-bottles",
    "cosmetic-tubes",
    "cosmetic-jars",
    "plastic-packaging",
    "glass-packaging",
    "packaging-materials",
]

COUNTRIES = {
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

PACKAGING_KEYWORDS = [
    "packaging", "bottle", "jar", "tube", "container", "pump", "dispenser",
    "dropper", "sprayer", "closure", "cap", "airless", "lotion bottle",
    "cream jar", "serum bottle", "plastic bottle", "glass bottle",
    "cosmetic packaging", "packaging manufacturer", "packaging supplier",
]

EXCLUDE_KEYWORDS = [
    "lipstick", "lip gloss", "mascara", "eyeliner", "eyeshadow", "foundation",
    "blush", "powder", "nail polish", "perfume", "fragrance",
    "shampoo", "conditioner", "hair dye", "false lash", "wig",
]

JUNK_NAME_PATTERNS = [
    r'^\d+\s*Company\s*Images?$', r'^\.+$', r'^\d+$', r'^\d+\s*Product\s*Images?$',
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


@dataclass
class SupplierRecord:
    company_name: str
    website_url: str = ""
    country: str = ""
    email: str = ""
    source_directory: str = SOURCE_DIRECTORY
    profile_url: str = ""


def clean_email(email: str) -> Optional[str]:
    if not email:
        return None
    match = re.match(r'([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})([A-Z].*)?', email)
    return match.group(1) if match else email


def extract_email_from_html(html: str) -> Optional[str]:
    """Extract email from HTML - checks footer first, then full page."""
    if not html:
        return None
    text = unescape(html)
    
    # Check footer sections first
    footer_patterns = [
        r'<footer[^>]*>(.*?)</footer>',
        r'<div[^>]*class="[^"]*footer[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*id="[^"]*footer[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*class="[^"]*contact[^"]*"[^>]*>(.*?)</div>',
    ]
    for pattern in footer_patterns:
        match = re.search(pattern, text, re.I | re.DOTALL)
        if match:
            section = match.group(1)
            email = _find_email_in_text(section)
            if email:
                return email
    
    # Check whole page
    return _find_email_in_text(text)


def _find_email_in_text(text: str) -> Optional[str]:
    """Find valid email in text."""
    text = text.lower()
    
    # mailto first
    mailto = re.search(r"mailto:([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})", text, re.I)
    if mailto:
        email = mailto.group(1)
        if is_useful_email(email):
            return email
    
    # Remove HTML and normalize
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
    if not re.search(r'\.(com|net|org|cn|kr|jp|tw|vn|th|sg|my|hk|co\.\w+)$', email):
        return False
    blocked = ["ec21.com", "ecplaza.net", "alibaba.com", "made-in-china.com", "example.com"]
    if any(b in email.lower() for b in blocked):
        return False
    junk = ["cloudflare", "404", "notfound", "blocked", "error",
            "ordercreditreport", "copyright", "@anytime", "@theforefront",
            "@homeandabroad", "@thistime", "@www", "pleasefeel"]
    if any(j in email.lower() for j in junk):
        return False
    return True


def fetch_page(url: str, timeout: int = WEBSITE_TIMEOUT) -> Optional[str]:
    """Fetch a page with proper timeout. Returns HTML or None."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
        return None
    except Exception:
        return None


def lookup_email_on_website(website_url: str) -> str:
    """Try multiple contact page URLs until email is found."""
    if not website_url:
        return ""
    
    base = website_url.rstrip("/")
    
    for path in CONTACT_PATHS:
        url = f"{base}{path}" if path else base
        
        html = fetch_page(url)
        if html:
            email = extract_email_from_html(html)
            if email:
                return email
    
    return ""


def random_delay():
    time.sleep(random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))


def is_junk_company_name(name: str) -> bool:
    name = name.strip()
    for pattern in JUNK_NAME_PATTERNS:
        if re.match(pattern, name, re.I):
            return True
    return len(name) < 3


def is_plausible_website(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.netloc or "").lower()
    if not host or any(x in host for x in ["ec21.com", "ecplaza.net", "alibaba.com"]):
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


def strip_tags(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", text))).strip()


def build_url(category: str, country_code: str, page: int) -> str:
    if page <= 1:
        return f"{BASE_DOMAIN}/companies/{country_code}/{category}.html"
    return f"{BASE_DOMAIN}/companies/{country_code}/{category}/page-{page}.html"


def fetch_html(fetcher, url: str) -> tuple[int, str]:
    try:
        response = fetcher.fetch(url)
        status = getattr(response, "status_code", None) or getattr(response, "status", 200)
        body = getattr(response, "body", None)
        if isinstance(body, (bytes, bytearray)):
            return int(status), body.decode("utf-8", errors="ignore")
        if isinstance(body, str) and body:
            return int(status), body
        if hasattr(response, "text"):
            return int(status), response.text
        return int(status), str(response)
    except:
        return 0, ""


def parse_company_listings(html: str) -> list[dict]:
    companies = []
    seen = set()
    pattern = r'<a[^>]*href="(https?://([^.]+)\.en\.ec21\.com/company_info\.html)"[^>]*>(.*?)</a>'
    for match in re.findall(pattern, html, re.I | re.DOTALL):
        profile_url = match[0]
        name = strip_tags(match[2])
        if not name or is_junk_company_name(name):
            continue
        clean_name = name.lower().strip()
        if clean_name in seen:
            continue
        seen.add(clean_name)
        pos = html.find(profile_url)
        context = html[max(0, pos-2000):min(len(html), pos+2000)] if pos > -1 else ""
        desc = ""
        desc_match = re.search(r'<p[^>]*>(.*?)</p>', context, re.I | re.DOTALL)
        if desc_match:
            desc = strip_tags(desc_match.group(1))
        companies.append({"name": name, "profile_url": profile_url, "description": desc})
    return companies


def is_packaging_supplier(description: str) -> bool:
    if not description:
        return True
    desc = description.lower()
    has_pkg = any(kw in desc for kw in PACKAGING_KEYWORDS)
    is_product = any(kw in desc for kw in EXCLUDE_KEYWORDS)
    return has_pkg or not is_product


def extract_website_from_profile(html: str) -> str:
    patterns = [
        r'Website:?\s*</[^>]+>\s*<[^>]+>\s*<a[^>]+href="([^"]+)"',
        r'<a[^>]+href="(https?://(?!.*ec21\.com)[^"]+)"[^>]*>Website</a>',
        r'Homepage:?\s*<a[^>]+href="([^"]+)"',
        r'website[:\s]*<a[^>]+href="([^"]+)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, re.I | re.DOTALL)
        if match:
            url = normalize_url(match.group(1))
            if is_plausible_website(url):
                return url
    link_pattern = r'href="(https?://(?!.*ec21\.com)[^"]+)"'
    for match in re.findall(link_pattern, html, re.I):
        url = normalize_url(match)
        if is_plausible_website(url):
            return url
    return ""


def enrich_single_profile(args):
    norm_name, record = args
    local_fetcher = ScraplingStealthFetcher(browser_engine="camoufox")
    try:
        _, html = fetch_html(local_fetcher, record.profile_url)
        if html:
            website = extract_website_from_profile(html)
            if website and not record.website_url:
                record.website_url = website
            email = extract_email_from_html(html)
            email = clean_email(email)
            if email and is_useful_email(email) and not record.email:
                record.email = email
    except:
        pass
    return norm_name, record


def save_checkpoint(records: list[SupplierRecord], path: str):
    if not records:
        return
    df = pd.DataFrame([asdict(r) for r in records])
    df["name_norm"] = df["company_name"].str.lower().str.strip()
    df = df.drop_duplicates(subset=["name_norm"]).drop(columns=["name_norm"])
    df.to_csv(path, index=False, encoding="utf-8-sig", sep='\t')


def main():
    print("=" * 60)
    print("EC21 Supplier Scraper (Simplified)")
    print(f"Target: {TARGET_SUPPLIERS} | Contact paths: {len(CONTACT_PATHS)}")
    print("=" * 60)
    
    all_records = []
    seen_names = set()
    save_counter = 0
    
    print("\n[PHASE 1] Scraping companies...")
    
    for category in CATEGORIES:
        if len(all_records) >= TARGET_SUPPLIERS:
            break
        for country_name, country_code in COUNTRIES.items():
            if len(all_records) >= TARGET_SUPPLIERS:
                break
            print(f"\n[{category}] [{country_name}]")
            consecutive_zero_pages = 0
            
            for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
                if len(all_records) >= TARGET_SUPPLIERS:
                    break
                
                url = build_url(category, country_code, page)
                list_fetcher = ScraplingStealthFetcher(browser_engine="camoufox")
                
                try:
                    status, html = fetch_html(list_fetcher, url)
                except Exception as e:
                    print(f"  Error: {e}")
                    break
                
                if status in (404, 403) or not html:
                    consecutive_zero_pages += 1
                    if consecutive_zero_pages >= ZERO_NEW_PAGES_CUTOFF:
                        break
                    continue
                
                companies = parse_company_listings(html)
                if not companies:
                    consecutive_zero_pages += 1
                    if consecutive_zero_pages >= ZERO_NEW_PAGES_CUTOFF:
                        break
                    continue
                
                consecutive_zero_pages = 0
                page_records = []
                for company in companies:
                    norm_name = company["name"].lower().strip()
                    if norm_name in seen_names:
                        continue
                    if not is_packaging_supplier(company.get("description", "")):
                        continue
                    record = SupplierRecord(
                        company_name=company["name"],
                        country=country_name,
                        profile_url=company["profile_url"],
                    )
                    page_records.append((norm_name, record))
                
                if not page_records:
                    continue
                
                page_new = 0
                with ThreadPoolExecutor(max_workers=PROFILE_WORKERS) as executor:
                    futures = {executor.submit(enrich_single_profile, pr): pr for pr in page_records}
                    for future in as_completed(futures):
                        norm_name, record = future.result()
                        if norm_name in seen_names:
                            continue
                        seen_names.add(norm_name)
                        all_records.append(record)
                        page_new += 1
                        save_counter += 1
                        if save_counter >= AUTOSAVE_EVERY_NEW_RECORDS:
                            save_checkpoint(all_records, PARTIAL_SCRAPE_CSV)
                            print(f"  [SAVE] {len(all_records)} records")
                            save_counter = 0
                
                print(f"  Page {page}: +{page_new} (Total: {len(all_records)})")
                random_delay()
    
    save_checkpoint(all_records, PARTIAL_SCRAPE_CSV)
    print(f"\n[PHASE 1 DONE] {len(all_records)} companies. Saved to {PARTIAL_SCRAPE_CSV}")
    
    # PHASE 2: EMAIL ENRICHMENT
    candidates = [r for r in all_records if r.website_url and not r.email]
    print(f"\n[PHASE 2] Enriching {len(candidates)} websites...")
    print(f"  Strategy: Try {len(CONTACT_PATHS)} contact page variations")
    
    found = 0
    skipped = 0
    
    with ThreadPoolExecutor(max_workers=WEBSITE_EMAIL_WORKERS) as executor:
        futures = {executor.submit(lookup_email_on_website, r.website_url): r for r in candidates}
        
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
                print(f"  [{found}] {record.company_name[:40]} → {email}")
                
                if found % AUTOSAVE_EVERY_NEW_EMAILS == 0:
                    save_checkpoint(all_records, PARTIAL_ENRICH_CSV)
                    print(f"  [SAVE] {found} emails → {PARTIAL_ENRICH_CSV}")
            
            if i % 25 == 0:
                print(f"  Progress: {i}/{len(candidates)}, found {found}, skipped {skipped}")
    
    save_checkpoint(all_records, PARTIAL_ENRICH_CSV)
    
    # FINAL OUTPUT
    df = pd.DataFrame([asdict(r) for r in all_records])
    df["name_norm"] = df["company_name"].str.lower().str.strip()
    df = df.drop_duplicates(subset=["name_norm"]).drop(columns=["name_norm"])
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig", sep='\t')
    
    df_clean = df[df['email'].notna() & (df['email'] != '')]
    df_clean.to_csv(CLEANED_CSV, index=False, encoding="utf-8-sig", sep='\t')
    
    print(f"\n{'='*60}")
    print(f"DONE!")
    print(f"  Raw: {OUTPUT_CSV} ({len(df)} suppliers)")
    print(f"  With email: {(df['email'] != '').sum()}")
    print(f"  Cleaned: {CLEANED_CSV} ({len(df_clean)} suppliers)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()