"""
poist.ie — Irish Language Job Scraper v3
Targets 11 verified Irish language job sources directly.
Runs nightly via GitHub Actions — free.
"""

import os
import requests
from bs4 import BeautifulSoup
from datetime import date
import time

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# Headers for Supabase REST API
HEADERS_SB = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

IRISH_KEYWORDS = [
    "gaeilge", "irish language", "líofa", "as gaeilge", "gaeltacht",
    "irish speaker", "fluent irish", "native irish", "inniúlacht",
    "oifigeach gaeilge", "irish officer", "bilingual", "dátheangach",
    "rannóg an aistriúcháin", "aistritheoir", "ateangaire",
]

SCRAPE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; poist.ie-bot/1.0; +https://poist.ie)"
}

def is_irish_keyword_match(title: str, description: str = "") -> bool:
    text = (title + " " + description).lower()
    return any(kw in text for kw in IRISH_KEYWORDS)

def url_already_exists(source_url: str) -> bool:
    try:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/jobs",
            headers=HEADERS_SB,
            params={"source_url": f"eq.{source_url}", "select": "id"},
            timeout=10,
        )
        return resp.status_code == 200 and len(resp.json()) > 0
    except Exception:
        return False

def insert_job(job: dict) -> bool:
    """Inserts job into Supabase with improved error reporting."""
    payload = {
        "title":         job["title"],
        "description":   job.get("description", ""),
        "county":        job.get("county", ""),
        "sector":        job.get("sector", ""),
        "status":        "active",
        "is_aggregated": True,
        "source_url":    job["source_url"],
        "source_name":   job["source_name"],
    }
    # Remove None values
    payload = {k: v for k, v in payload.items() if v is not None}
    
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/jobs",
        headers=HEADERS_SB,
        json=payload,
        timeout=10,
    )
    
    if resp.status_code in (200, 201):
        print(f"  INSERTED: {job['title']} [{job['source_name']}]")
        return True
    else:
        # We need to see why the DB is rejecting the row (e.g., missing employer_id)
        try:
            err_msg = resp.json()
        except:
            err_msg = resp.text
        print(f"  FAILED ({resp.status_code}): {job['title']} -- {err_msg}")
        return False

def scrape_page(url: str, source_name: str, sector: str,
                base_url: str, href_keywords: list,
                title_filter: bool = False) -> list:
    """Reusable scraper with blacklist filtering for junk data."""
    jobs = []
    
    # UI elements and categories to ignore
    BLACKLIST = [
        "gaillimh", "maigh eo", "baile átha cliath", "cill mhantáin", 
        "loch garman", "aontroim", "dún na ngall", "thar lear", 
        "timpeall na tíre", "folúntais", "cláraigh", "older entries", 
        "nua-iontrálacha", "next page", "previous page", "contact", 
        "resources", "faisnéis", "eolas", "search", "nuachtlitir", "privacy"
    ]

    try:
        resp = requests.get(url, headers=SCRAPE_HEADERS, timeout=15)
        print(f"  {source_name}: status {resp.status_code}")
        if resp.status_code != 200:
            return []
            
        soup = BeautifulSoup(resp.text, "html.parser")
        seen = set()
        
        for link in soup.find_all("a", href=True):
            title = link.get_text(strip=True)
            href  = link.get("href", "")

            # 1. Skip junk titles and short strings
            if not title or len(title) < 12 or len(title) > 200:
                continue
            
            if any(bad_word in title.lower() for bad_word in BLACKLIST) or "«" in title or "»" in title:
                continue

            # 2. Skip non-job links
            if not href or href.startswith("#") or "javascript:" in href:
                continue

            # 3. Check for required keywords in the URL
            if href_keywords and not any(w in href.lower() for w in href_keywords):
                continue

            # 4. Construct full URL
            full_url = href if href.startswith("http") else f"{base_url.rstrip('/')}{href if href.startswith('/') else '/' + href}"
            
            if full_url in seen:
                continue
            seen.add(full_url)

            # 5. Optional Irish keyword filter
            if title_filter and not is_irish_keyword_match(title):
                continue

            jobs.append({
                "title":       title,
                "description": f"Role advertised on {source_name}.",
                "county":      "",
                "sector":      sector,
                "source_url":  full_url,
                "source_name": source_name,
            })
            
        print(f"  {source_name}: found {len(jobs)} jobs")
        time.sleep(2)
    except Exception as e:
        print(f"  {source_name} error: {e}")
    return jobs

# --- SCRAPER SOURCES ---

def scrape_peig():
    return scrape_page("https://peig.ie/foluntais/", "peig.ie", "Irish Language Community", "https://peig.ie", ["foluntais", "job", "post", "peig.ie"])

def scrape_foras():
    return scrape_page("https://www.forasnagaeilge.ie/foluntais/", "forasnagaeilge.ie", "Irish Language", "https://www.forasnagaeilge.ie", ["foluntais", "job", "post"])

def scrape_udaras():
    return scrape_page("https://udaras.ie/foluntais/", "udaras.ie", "Gaeltacht Development", "https://udaras.ie", ["foluntais", "job", "post"])

def scrape_cnag():
    return scrape_page("https://cnag.ie/ga/eolas-faoin-gconradh/foluntais/", "cnag.ie", "Irish Language Promotion", "https://cnag.ie", ["foluntais", "post", "job"])

def scrape_gaeloideachas():
    return scrape_page("https://gaeloideachas.ie/foluntais/", "gaeloideachas.ie", "Education", "https://gaeloideachas.ie", ["foluntais", "post", "teagasc"])

def scrape_coimisineir():
    return scrape_page("https://www.coimisineir.ie/index.cfm?page=vacancies", "coimisineir.ie", "Irish Language", "https://www.coimisineir.ie", ["vacanc", "foluntais", "post"])

def scrape_oireachtas():
    return scrape_page("https://www.oireachtas.ie/en/about/careers/", "oireachtas.ie", "Government", "https://www.oireachtas.ie", ["career", "job", "role"])

def scrape_localgovt():
    jobs = []
    for term in ["gaeilge", "irish"]:
        jobs += scrape_page(f"https://www.localgovernmentjobs.ie/jobs?keywords={term}", "localgovernmentjobs.ie", "Local Government", "https://www.localgovernmentjobs.ie", ["/job/", "jobid"], True)
    return jobs

def scrape_hse():
    jobs = []
    for term in ["gaeilge", "irish"]:
        jobs += scrape_page(f"https://careerhub.hse.ie/candidates/jobs/search?keywords={term}", "careerhub.hse.ie", "Health", "https://careerhub.hse.ie", ["/job/", "jobid"], True)
    return jobs

def scrape_jobsireland():
    return scrape_page("https://jobsireland.ie/en-US/Search?term=gaeilge", "jobsireland.ie", "General", "https://jobsireland.ie", ["/job/", "jobId"], True)

def scrape_adzuna():
    app_id, app_key = os.environ.get("ADZUNA_APP_ID", ""), os.environ.get("ADZUNA_APP_KEY", "")
    if not app_id or not app_key: return []
    jobs = []
    for term in ["gaeilge", "irish speaker ireland"]:
        try:
            resp = requests.get("https://api.adzuna.com/v1/api/jobs/ie/search/1", params={"app_id": app_id, "app_key": app_key, "what": term, "results_per_page": 10}, timeout=15)
            for item in resp.json().get("results", []):
                jobs.append({"title": item.get("title", ""), "description": "Via Adzuna.", "county": item.get("location", {}).get("display_name", ""), "sector": item.get("category", {}).get("label", ""), "source_url": item.get("redirect_url", ""), "source_name": "adzuna.ie"})
        except: pass
    return jobs

def cleanup_expired_jobs():
    today = date.today().isoformat()
    try:
        requests.delete(f"{SUPABASE_URL}/rest/v1/jobs", headers=HEADERS_SB, params={"is_aggregated": "eq.true", "closing_date": f"lt.{today}"}, timeout=10)
    except: pass

def main():
    print("poist.ie job scraper v3 — 11 sources\n")
    scrapers = [
        ("PEIG.ie", scrape_peig), ("Foras na Gaeilge", scrape_foras),
        ("Udaras na Gaeltachta", scrape_udaras), ("Conradh na Gaeilge", scrape_cnag),
        ("Gaeloideachas", scrape_gaeloideachas), ("Coimisineir Teanga", scrape_coimisineir),
        ("Houses of Oireachtas", scrape_oireachtas), ("Local Government Jobs", scrape_localgovt),
        ("HSE Career Hub", scrape_hse), ("JobsIreland.ie", scrape_jobsireland),
        ("Adzuna", scrape_adzuna),
    ]

    all_jobs = []
    for name, fn in scrapers:
        print(f"Checking {name}...")
        all_jobs += fn()

    print(f"\nFound {len(all_jobs)} potential jobs\n")

    inserted = skipped = 0
    for job in all_jobs:
        if not job.get("source_url") or not job.get("title"):
            skipped += 1
            continue
        if url_already_exists(job["source_url"]):
            skipped += 1
            continue
        if insert_job(job):
            inserted += 1
        time.sleep(0.3)

    print(f"\nDone -- {inserted} inserted, {skipped} skipped")
    cleanup_expired_jobs()

if __name__ == "__main__":
    main()