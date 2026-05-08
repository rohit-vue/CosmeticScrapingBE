"""
ExportPages supplier scraper - Upgraded with all improvements.
Footer-first email extraction, 23 contact paths, 15s timeout, progressive saves.
"""

from __future__ import annotations

import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from html import unescape
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from playwright.sync_api import sync_playwright

# ===== CONFIGURATION =====
SOURCE_DIRECTORY = "ExportPages"
CATEGORY = "142"
OUTPUT_CSV = "exportpages_suppliers_raw.csv"
CLEANED_CSV = "exportpages_suppliers_cleaned.csv"
PARTIAL_SCRAPE_CSV = "exportpages_suppliers_scrape_progress.csv"
PARTIAL_ENRICH_CSV = "exportpages_suppliers_enrich_progress.csv"
TARGET_SUPPLIERS = 500
AUTOSAVE_EVERY_NEW_RECORDS = 10
AUTOSAVE_EVERY_NEW_EMAILS = 10
MAX_PAGES_PER_COUNTRY = 5

COUNTRIES = {
    "China": "44", "South Korea": "125", "Taiwan": "196", "Japan": "107",
    "Vietnam": "232", "Thailand": "198", "Singapore": "173",
    "Malaysia": "127", "Hong Kong": "94",
}

WEBSITE_TIMEOUT = 15
WEBSITE_EMAIL_WORKERS = 5

# Extended contact paths
CONTACT_PATHS = [
    "", "/contact", "/contact-us", "/contactus", "/contact.html",
    "/contact-us.html", "/contactus.html", "/about", "/about-us",
    "/about.html", "/about-us.html", "/contact/", "/contact-us/",
    "/contactus/", "/about/", "/about-us/", "/contactinfo",
    "/contact-info", "/contact_info", "/get-in-touch", "/reach-us",
    "/en/contact", "/en/contact-us",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

JUNK_EMAIL_PHRASES = [
    "cloudflare", "404", "notfound", "blocked", "error",
    "ordercreditreport", "copyright", "@anytime", "@theforefront",
    "@homeandabroad", "@thistime", "@www", "pleasefeel"
]


# ===== HELPER FUNCTIONS =====

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


def is_useful_email(email: str) -> bool:
    if not email or len(email) > 50 or len(email) < 6:
        return False
    if ' ' in email or email.count('@') != 1:
        return False
    if not re.search(r'\.(com|net|org|cn|kr|jp|tw|vn|th|sg|my|hk|co\.\w+)$', email):
        return False
    blocked = ["exportpages.com", "alibaba.com", "made-in-china.com", "example.com"]
    if any(b in email.lower() for b in blocked):
        return False
    if any(j in email.lower() for j in JUNK_EMAIL_PHRASES):
        return False
    return True


def fetch_page(url: str, timeout: int = WEBSITE_TIMEOUT) -> Optional[str]:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
        return None
    except:
        return None


def lookup_email(website_url: str) -> str:
    if not website_url or str(website_url) == 'nan':
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


def save_checkpoint(results, path):
    if not results:
        return
    df = pd.DataFrame(results)
    df["name_norm"] = df["company_name"].str.lower().str.strip()
    df = df.drop_duplicates(subset=["name_norm"]).drop(columns=["name_norm"])
    df.to_csv(path, index=False, encoding="utf-8-sig", sep='\t')


def load_existing_data():
    if Path(OUTPUT_CSV).exists():
        df = pd.read_csv(OUTPUT_CSV, sep='\t')
        print(f"[RESUME] Loaded {len(df)} existing suppliers")
        return df.to_dict('records'), set(df['company_name'].str.lower().str.strip())
    return [], set()


# ===== PLAYWRIGHT FUNCTIONS =====

def get_company_ids(page, country_id):
    """Get unique company IDs from listing pages."""
    all_ids = set()
    
    for page_num in range(1, MAX_PAGES_PER_COUNTRY + 1):
        url = f"https://exportpages.com/cat/{CATEGORY}?page={page_num}&CompanySearch%5Bcountry_id%5D={country_id}"
        
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(2)
        page.keyboard.press("Escape")
        time.sleep(1)
        try:
            page.locator('button[aria-label="Close"]').click(timeout=2000)
            time.sleep(1)
        except:
            pass
        
        for i in range(5):
            page.evaluate("window.scrollBy(0, 600)")
            time.sleep(0.5)
        
        html = page.content()
        ids = set(re.findall(r'/comp/(\d+)', html))
        new_ids = ids - all_ids
        all_ids.update(ids)
        
        print(f"  Page {page_num}: {len(new_ids)} new (Total: {len(all_ids)})")
        
        if not new_ids:
            break
        
        time.sleep(random.uniform(1, 2))
    
    return list(all_ids)


def get_company_details(page, company_id):
    """Visit profile, extract name and website."""
    url = f"https://exportpages.com/comp/{company_id}"
    
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(2)
        page.keyboard.press("Escape")
        time.sleep(1)
        try:
            page.locator('button[aria-label="Close"]').click(timeout=2000)
            time.sleep(1)
        except:
            pass
        
        html = page.content()
        text = page.inner_text('body')
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        
        name = ""
        for line in lines[:15]:
            if len(line) > 5 and line not in ['Categories', 'BECOME A PROVIDER', 'MY ACCOUNT', 'Exportpages']:
                name = line
                break
        
        websites = re.findall(r'href="(https?://(?!.*exportpages)[^"]+)"', html)
        real_sites = [w for w in websites if not any(x in w for x in
                      ['facebook', 'twitter', 'linkedin', 'instagram', 'sendinblue'])]
        website = real_sites[0] if real_sites else ""
        
        return {"name": name, "website": website}
    
    except:
        return {"name": "", "website": ""}


# ===== MAIN =====

def main():
    print("=" * 60)
    print("ExportPages Supplier Scraper (Upgraded)")
    print(f"Contact paths: {len(CONTACT_PATHS)} | Timeout: {WEBSITE_TIMEOUT}s")
    print("=" * 60)
    
    all_results, seen_names = load_existing_data()
    seen_ids = set()
    save_counter = 0
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = context.new_page()
        
        try:
            for country_name, country_id in COUNTRIES.items():
                if len(all_results) >= TARGET_SUPPLIERS:
                    break
                
                print(f"\n[{country_name}]")
                
                ids = get_company_ids(page, country_id)
                new_ids = [i for i in ids if i not in seen_ids]
                print(f"  New companies: {len(new_ids)}")
                
                for cid in new_ids:
                    if len(all_results) >= TARGET_SUPPLIERS:
                        break
                    
                    details = get_company_details(page, cid)
                    seen_ids.add(cid)
                    
                    if not details['name']:
                        continue
                    
                    name_norm = details['name'].lower().strip()
                    if name_norm in seen_names:
                        continue
                    seen_names.add(name_norm)
                    
                    record = {
                        "company_name": details['name'],
                        "country": country_name,
                        "website_url": details['website'],
                        "email": "",
                        "profile_url": f"https://exportpages.com/comp/{cid}",
                        "source_directory": SOURCE_DIRECTORY,
                    }
                    
                    all_results.append(record)
                    save_counter += 1
                    
                    print(f"  + {details['name'][:50]} → {details['website'][:50]}")
                    
                    if save_counter >= AUTOSAVE_EVERY_NEW_RECORDS:
                        save_checkpoint(all_results, PARTIAL_SCRAPE_CSV)
                        print(f"  [SAVE] {len(all_results)} records → {PARTIAL_SCRAPE_CSV}")
                        save_counter = 0
                    
                    time.sleep(random.uniform(0.5, 1))
        
        except KeyboardInterrupt:
            print("\n[INTERRUPTED] Saving...")
            save_checkpoint(all_results, PARTIAL_SCRAPE_CSV)
        
        browser.close()
    
    # Phase 2: Email enrichment with requests
    candidates = [r for r in all_results if r.get('website_url') and not r.get('email')]
    print(f"\n[EMAILS] Enriching {len(candidates)} websites...")
    print(f"  Strategy: Footer → Homepage → {len(CONTACT_PATHS)} contact paths")
    
    found = 0
    skipped = 0
    emails_since_save = 0
    
    with ThreadPoolExecutor(max_workers=WEBSITE_EMAIL_WORKERS) as executor:
        futures = {executor.submit(lookup_email, r['website_url']): i for i, r in enumerate(candidates)}
        total = len(candidates)
        
        for i, future in enumerate(as_completed(futures), 1):
            idx = futures[future]
            try:
                email = future.result(timeout=WEBSITE_TIMEOUT + 5)
            except FutureTimeoutError:
                skipped += 1
                continue
            
            if email:
                candidates[idx]['email'] = email
                found += 1
                emails_since_save += 1
                
                if emails_since_save >= AUTOSAVE_EVERY_NEW_EMAILS:
                    save_checkpoint(all_results, PARTIAL_ENRICH_CSV)
                    print(f"  [SAVE] {found} emails → {PARTIAL_ENRICH_CSV}")
                    emails_since_save = 0
            
            if i % 25 == 0:
                print(f"  Progress: {i}/{total}, found {found}, skipped {skipped}")
    
    save_checkpoint(all_results, PARTIAL_ENRICH_CSV)
    
    # Final output
    df = pd.DataFrame(all_results)
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


if __name__ == "__main__":
    main()