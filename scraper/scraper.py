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
    """Inserts job into Supabase. Note: 'status' must be 'approved' per DB constraints."""
    payload = {
        "title":         job["title"],
        "description":   job.get("description", ""),
        "county":        job.get("county", ""),
        "sector":        job.get("sector", ""),
        "status":        "approved", 
        "is_aggregated": True,
        "source_url":    job["source_url"],
        "source_name":   job["source_name"],
    }
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
        try:
            err_msg = resp.json()
        except:
            err_msg = resp.text
        print(f"  FAILED ({resp.status_code}): {job['title']} -- {err_msg}")
        return False

def scrape_page(url: str, source_name: str, sector: str,
                base_url: str, href_keywords: list,
                title_filter: bool = False) -> list:
    """Reusable scraper with strict blacklist filtering."""
    jobs = []
    
    BLACKLIST = [
        "gaillimh", "maigh eo", "baile átha cliath", "cill mhantáin", 
        "loch garman", "aontroim", "dún na ngall", "thar lear", 
        "timpeall na tíre", "folúntais", "cláraigh", "older entries", 
        "nua-iontrálacha", "next page", "previous page", "contact", 
        "resources", "faisnéis", "eolas", "search", "nuachtlitir", "privacy",
        "an tAontas Eorpach", "post-primary schools", "primary schools"
    ]

    try:
        resp = requests.get(url, headers=SCRAPE_HEADERS, timeout=15)
        if resp.status_code != 200:
            print(f"  {source_name}: status {resp.status_code} (skipped)")
            return []
            
        soup = BeautifulSoup(resp.text, "html.parser")
        seen = set()
        
        for link in soup.find_all("a", href=True):
            title = link.get_text(strip=True)
            href  = link.get("href", "")

            if not title or len(title) < 12 or len(title) > 200:
                continue
            
            if any(bad.lower() in title.lower() for bad in BLACKLIST) or "«" in title or "»" in title:
                continue

            if not href or href.startswith("#") or "javascript:" in href:
                continue

            if href_keywords and not any(w in href.lower() for w in href_keywords):
                continue

            full_url = href if href.startswith("http") else f"{base_url.rstrip('/')}{href if href.startswith('/') else '/' + href}"
            
            if full_url in seen:
                continue
            seen.add(full_url)

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
            
        print(f"  {source_name}: found {len(jobs)} potential links")
        time.sleep(1)
    except Exception as e:
        print(f"  {source_name} error: {e}")
    return jobs

# --- SCRAPER SOURCES ---

def scrape_peig():
    return scrape_page("https://sceal.ie/foluntais/", "peig.ie", "Irish Language Community", "https://sceal.ie", ["job", "post", "foluntais"])

def scrape_foras():
    return scrape_page("https://www.forasnagaeilge.ie/about-foras-na-gaeilge/vacancies/?lang=en", "forasnagaeilge.ie", "Irish Language", "https://www.forasnagaeilge.ie", ["vacanc", "job", "post"])

def scrape_udaras():
    jobs = []
    # Source 1: Client companies
    jobs += scrape_page("https://udaras.ie/oiliuint-fostaiocht/fostaiocht/cliantchomhlachtai-an-udarais-sa-ghaeltacht/", "udaras.ie (cliant)", "Gaeltacht Development", "https://udaras.ie", ["job", "foluntas", "post"])
    # Source 2: Internal Udaras roles
    jobs += scrape_page("https://udaras.ie/oiliuint-fostaiocht/fostaiocht/poist-udaras-na-gaeltachta/", "udaras.ie (poist)", "Gaeltacht Development", "https://udaras.ie", ["job", "foluntas", "post"])
    return jobs

def scrape_cnag():
    # Attempted fix for redirect/moved page
    return scrape_page("https://cnag.ie/ga/eolas/foluntais.html", "cnag.ie", "Irish Language Promotion", "https://cnag.ie", ["foluntais", "post", "job"])

def scrape_gaeloideachas():
    return scrape_page("https://gaeloideachas.ie/foluntais/", "gaeloideachas.ie", "Education", "https://gaeloideachas.ie", ["foluntais", "post"])

def scrape_coimisineir():
    return scrape_page("https://www.coimisineir.ie/index.cfm?page=vacancies", "coimisineir.ie", "Irish Language", "https://www.coimisineir.ie", ["vacanc", "foluntais"])

def scrape_localgovt():
    jobs = []
    for term in ["gaeilge", "irish"]:
        jobs += scrape_page(f"https://www.localgovernmentjobs.ie/jobs?keywords={term}", "localgovernmentjobs.ie", "Local Government", "https://www.localgovernmentjobs.ie", ["/job/", "jobid"], True)
    return jobs

def scrape_hse():
    return scrape_page("https://about.hse.ie/jobs/job-search/", "careerhub.hse.ie", "Health", "https://about.hse.ie", ["job", "vacanc"], True)

def scrape_jobsireland():
    # Note: If this still 404s, they likely block standard scrapers
    return scrape_page("https://jobsireland.ie/en-US/Search?term=gaeilge", "jobsireland.ie", "General", "https://jobsireland.ie", ["/job/", "jobId"], True)

def scrape_adzuna():
    app_id, app_key = os.environ.get("ADZUNA_APP_ID", ""), os.environ.get("ADZUNA_APP_KEY", "")
    if not app_id or not app_key: return []
    jobs = []
    for term in ["gaeilge", "irish speaker ireland"]:
        try:
            resp = requests.get("https://api.adzuna.com/v1/api/jobs/ie/search/1", params={"app_id": app_id, "app_key": app_key, "what": term, "results_per_page": 5}, timeout=15)
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
    print("poist.ie job scraper v3 — Running...\n")
    scrapers = [
        ("PEIG/Sceal", scrape_peig), ("Foras na Gaeilge", scrape_foras),
        ("Udaras na Gaeltachta", scrape_udaras), ("Conradh na Gaeilge", scrape_cnag),
        ("Gaeloideachas", scrape_gaeloideachas), ("Coimisineir Teanga", scrape_coimisineir),
        ("Local Government Jobs", scrape_localgovt), ("HSE", scrape_hse),
        ("JobsIreland.ie", scrape_jobsireland), ("Adzuna", scrape_adzuna),
    ]

    all_jobs = []
    for name, fn in scrapers:
        print(f"Checking {name}...")
        all_jobs += fn()

    print(f"\nFound {len(all_jobs)} total potential links\n")

    inserted = skipped = 0
    for job in all_jobs:
        if not job.get("source_url") or not job.get("title"):
            continue
        if url_already_exists(job["source_url"]):
            skipped += 1
            continue
        if insert_job(job):
            inserted += 1
        time.sleep(0.3)

    print(f"\nFinal Stats: {inserted} inserted, {skipped} skipped")
    cleanup_expired_jobs()

if __name__ == "__main__":
    main()