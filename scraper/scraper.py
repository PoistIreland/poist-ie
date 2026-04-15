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
        print(f"  FAILED ({resp.status_code}): {job['title']} -- {resp.text[:120]}")
        return False


def scrape_page(url: str, source_name: str, sector: str,
                base_url: str, href_keywords: list,
                title_filter: bool = False) -> list:
    """Reusable scraper for simple HTML job listing pages."""
    jobs = []
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
            if not title or len(title) < 8 or len(title) > 200:
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
        print(f"  {source_name}: found {len(jobs)} jobs")
        time.sleep(2)
    except Exception as e:
        print(f"  {source_name} error: {e}")
    return jobs


# ── 1. PEIG.ie ────────────────────────────────────────────────────────────────
def scrape_peig() -> list:
    return scrape_page(
        url="https://peig.ie/foluntais/",
        source_name="peig.ie",
        sector="Irish Language Community",
        base_url="https://peig.ie",
        href_keywords=["foluntais", "job", "post", "vacanc", "career", "peig.ie"],
    )


# ── 2. Foras na Gaeilge ───────────────────────────────────────────────────────
def scrape_foras() -> list:
    return scrape_page(
        url="https://www.forasnagaeilge.ie/foluntais/",
        source_name="forasnagaeilge.ie",
        sector="Irish Language",
        base_url="https://www.forasnagaeilge.ie",
        href_keywords=["foluntais", "job", "post", "vacanc"],
    )


# ── 3. Údarás na Gaeltachta ───────────────────────────────────────────────────
def scrape_udaras() -> list:
    return scrape_page(
        url="https://udaras.ie/foluntais/",
        source_name="udaras.ie",
        sector="Gaeltacht Development",
        base_url="https://udaras.ie",
        href_keywords=["foluntais", "job", "career", "vacanc", "post"],
    )


# ── 4. Conradh na Gaeilge ─────────────────────────────────────────────────────
def scrape_cnag() -> list:
    return scrape_page(
        url="https://cnag.ie/ga/eolas-faoin-gconradh/foluntais/",
        source_name="cnag.ie",
        sector="Irish Language Promotion",
        base_url="https://cnag.ie",
        href_keywords=["foluntais", "post", "job", "vacanc"],
    )


# ── 5. Gaeloideachas ──────────────────────────────────────────────────────────
def scrape_gaeloideachas() -> list:
    return scrape_page(
        url="https://gaeloideachas.ie/foluntais/",
        source_name="gaeloideachas.ie",
        sector="Education",
        base_url="https://gaeloideachas.ie",
        href_keywords=["foluntais", "post", "job", "vacanc", "teagasc"],
    )


# ── 6. Oifig an Choimisinéara Teanga ─────────────────────────────────────────
def scrape_coimisineir() -> list:
    jobs = scrape_page(
        url="https://www.coimisineir.ie/index.cfm?page=vacancies",
        source_name="coimisineir.ie",
        sector="Irish Language",
        base_url="https://www.coimisineir.ie",
        href_keywords=["vacanc", "foluntais", "post", "job", "career"],
    )
    if not jobs:
        jobs = scrape_page(
            url="https://www.coimisineir.ie/foluntais/",
            source_name="coimisineir.ie",
            sector="Irish Language",
            base_url="https://www.coimisineir.ie",
            href_keywords=["vacanc", "foluntais", "post", "job"],
        )
    return jobs


# ── 7. Houses of the Oireachtas ───────────────────────────────────────────────
def scrape_oireachtas() -> list:
    return scrape_page(
        url="https://www.oireachtas.ie/en/about/careers/",
        source_name="oireachtas.ie",
        sector="Government",
        base_url="https://www.oireachtas.ie",
        href_keywords=["career", "job", "vacanc", "role", "recruit"],
    )


# ── 8. LocalGovernmentJobs.ie ─────────────────────────────────────────────────
def scrape_localgovt() -> list:
    jobs = []
    for term in ["gaeilge", "irish", "oifigeach"]:
        found = scrape_page(
            url=f"https://www.localgovernmentjobs.ie/jobs?keywords={term}",
            source_name="localgovernmentjobs.ie",
            sector="Local Government",
            base_url="https://www.localgovernmentjobs.ie",
            href_keywords=["/job/", "/vacancy/", "/post/", "/role/", "jobid"],
            title_filter=True,
        )
        jobs += found
        time.sleep(1)
    return jobs


# ── 9. HSE Career Hub ─────────────────────────────────────────────────────────
def scrape_hse() -> list:
    jobs = []
    for term in ["gaeilge", "irish", "bilingual"]:
        found = scrape_page(
            url=f"https://careerhub.hse.ie/candidates/jobs/search?keywords={term}",
            source_name="careerhub.hse.ie",
            sector="Health",
            base_url="https://careerhub.hse.ie",
            href_keywords=["/job/", "jobid", "job-detail", "vacancy", "post"],
            title_filter=True,
        )
        jobs += found
        time.sleep(1)
    return jobs


# ── 10. JobsIreland.ie ────────────────────────────────────────────────────────
def scrape_jobsireland() -> list:
    return scrape_page(
        url="https://jobsireland.ie/en-US/Search?term=gaeilge",
        source_name="jobsireland.ie",
        sector="General",
        base_url="https://jobsireland.ie",
        href_keywords=["/job/", "/Job/", "jobId", "job-detail"],
        title_filter=True,
    )


# ── 11. Adzuna API ────────────────────────────────────────────────────────────
def scrape_adzuna() -> list:
    app_id  = os.environ.get("ADZUNA_APP_ID", "").strip()
    app_key = os.environ.get("ADZUNA_APP_KEY", "").strip()
    if not app_id or not app_key:
        print("  Adzuna: no keys -- skipping")
        return []
    jobs = []
    for term in ["gaeilge", "irish language officer", "irish speaker ireland"]:
        try:
            resp = requests.get(
                "https://api.adzuna.com/v1/api/jobs/ie/search/1",
                params={"app_id": app_id, "app_key": app_key,
                        "what": term, "results_per_page": 20},
                timeout=15,
            )
            results = resp.json().get("results", [])
            print(f"  Adzuna ({term}): {len(results)} results")
            for item in results:
                redirect_url = item.get("redirect_url", "")
                if not redirect_url:
                    continue
                jobs.append({
                    "title":       item.get("title", ""),
                    "description": "Irish language role via Adzuna.",
                    "county":      item.get("location", {}).get("display_name", ""),
                    "sector":      item.get("category", {}).get("label", ""),
                    "source_url":  redirect_url,
                    "source_name": "adzuna.ie",
                })
            time.sleep(1)
        except Exception as e:
            print(f"  Adzuna error ({term}): {e}")
    return jobs


# ── CLEANUP ───────────────────────────────────────────────────────────────────
def cleanup_expired_jobs():
    today = date.today().isoformat()
    try:
        resp = requests.delete(
            f"{SUPABASE_URL}/rest/v1/jobs",
            headers=HEADERS_SB,
            params={"is_aggregated": "eq.true", "closing_date": f"lt.{today}"},
            timeout=10,
        )
        print(f"Cleanup: {resp.status_code}")
    except Exception as e:
        print(f"Cleanup error: {e}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print("poist.ie job scraper v3 — 11 sources\n")

    scrapers = [
        ("PEIG.ie",                    scrape_peig),
        ("Foras na Gaeilge",           scrape_foras),
        ("Udaras na Gaeltachta",       scrape_udaras),
        ("Conradh na Gaeilge",         scrape_cnag),
        ("Gaeloideachas",              scrape_gaeloideachas),
        ("Coimisineir Teanga",         scrape_coimisineir),
        ("Houses of Oireachtas",       scrape_oireachtas),
        ("Local Government Jobs",      scrape_localgovt),
        ("HSE Career Hub",             scrape_hse),
        ("JobsIreland.ie",             scrape_jobsireland),
        ("Adzuna",                     scrape_adzuna),
    ]

    all_jobs = []
    for name, fn in scrapers:
        print(f"Checking {name}...")
        all_jobs += fn()

    print(f"\nFound {len(all_jobs)} potential Irish language jobs\n")

    inserted = 0
    skipped  = 0

    for job in all_jobs:
        if not job.get("source_url") or not job.get("title"):
            skipped += 1
            continue
        if url_already_exists(job["source_url"]):
            print(f"  Already exists: {job['title']}")
            skipped += 1
            continue
        if insert_job(job):
            inserted += 1
        time.sleep(0.3)

    print(f"\nDone -- {inserted} inserted, {skipped} skipped")
    cleanup_expired_jobs()


if __name__ == "__main__":
    main()