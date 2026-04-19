"""
poist.ie — Irish Language Job Scraper v6
33 verified Irish language job sources.
Uses heading-based extraction for accurate job titles.
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

SCRAPE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; poist.ie-bot/1.0; +https://poist.ie)"
}

HEADING_BLACKLIST = [
    "folúntais", "foluntais", "vacancies", "current vacancies", "all vacancies",
    "jobs", "poist", "careers", "gairmeacha", "working with us",
    "home", "about", "contact", "news", "nuacht", "search",
    "no vacancies", "no current vacancies", "níl aon fholúntas",
    "apply now", "read more", "tuilleadh eolais", "find out more",
]

IRISH_LANGUAGE_KEYWORDS = [
    "gaeilge", "irish language", "gaeltacht", "líofa", "bilingual",
    "múinteoir", "teacher", "oifigeach", "officer", "aistritheoir",
    "translator", "ateangaire", "interpreter",
]


def is_valid_job_title(title: str) -> bool:
    t = title.strip()
    if len(t) < 8 or len(t) > 180:
        return False
    if t.lower() in [b.lower() for b in HEADING_BLACKLIST]:
        return False
    return True


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
        print(f"  FAILED ({resp.status_code}): {job['title']}")
        return False


def scrape_headings(url: str, source_name: str, sector: str,
                    base_url: str, irish_only: bool = False) -> list:
    """
    Extract job listings by targeting h2/h3 headings with links.
    Falls back to article/li elements if no headings found.
    Set irish_only=True for general sites that need keyword filtering.
    """
    jobs = []
    try:
        resp = requests.get(url, headers=SCRAPE_HEADERS, timeout=15)
        print(f"  {source_name}: status {resp.status_code}")
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        seen = set()

        # Strategy 1: h2/h3 headings containing links
        for tag in soup.find_all(["h2", "h3", "h4"]):
            link = tag.find("a", href=True)
            if not link:
                continue
            title    = tag.get_text(strip=True)
            href     = link.get("href", "")
            if not href or href.startswith("#") or "javascript:" in href:
                continue
            full_url = href if href.startswith("http") else f"{base_url.rstrip('/')}{href if href.startswith('/') else '/' + href}"
            if full_url in seen:
                continue
            seen.add(full_url)
            if not is_valid_job_title(title):
                continue
            if irish_only and not any(kw in title.lower() for kw in IRISH_LANGUAGE_KEYWORDS):
                continue
            jobs.append({
                "title": title,
                "description": f"Role advertised on {source_name}.",
                "county": "", "sector": sector,
                "source_url": full_url, "source_name": source_name,
            })

        # Strategy 2: article or list-item containers
        if not jobs:
            for container in soup.select("article, li.job, li.post, .job-listing, .vacancy-item, .job-item"):
                link = container.find("a", href=True)
                if not link:
                    continue
                title = link.get_text(strip=True)
                href  = link.get("href", "")
                if not href or not title:
                    continue
                full_url = href if href.startswith("http") else f"{base_url.rstrip('/')}{href if href.startswith('/') else '/' + href}"
                if full_url in seen:
                    continue
                seen.add(full_url)
                if not is_valid_job_title(title):
                    continue
                if irish_only and not any(kw in title.lower() for kw in IRISH_LANGUAGE_KEYWORDS):
                    continue
                jobs.append({
                    "title": title,
                    "description": f"Role advertised on {source_name}.",
                    "county": "", "sector": sector,
                    "source_url": full_url, "source_name": source_name,
                })

        print(f"  {source_name}: {len(jobs)} job titles found")
        time.sleep(2)

    except Exception as e:
        print(f"  {source_name} error: {e}")

    return jobs


# ── ALL 33 SOURCES ────────────────────────────────────────────────────────────

SOURCES = [
    # Irish Language Specific — all jobs relevant
    {
        "name": "sceal.ie",
        "fn": lambda: scrape_headings("https://sceal.ie/foluntais/", "sceal.ie", "Irish Language Community", "https://sceal.ie"),
    },
    {
        "name": "forasnagaeilge.ie",
        "fn": lambda: scrape_headings("https://www.forasnagaeilge.ie/about-foras-na-gaeilge/vacancies/?lang=en", "forasnagaeilge.ie", "Irish Language", "https://www.forasnagaeilge.ie"),
    },
    {
        "name": "udaras.ie",
        "fn": lambda: scrape_headings("https://www.udaras.ie/foluntais/", "udaras.ie", "Gaeltacht Development", "https://www.udaras.ie"),
    },
    {
        "name": "cnag.ie",
        "fn": lambda: scrape_headings("https://cnag.ie/en/info/conradh-na-gaeilge/facts-and-figures.html?view=article&id=1463:vacancy&catid=13", "cnag.ie", "Irish Language Promotion", "https://cnag.ie"),
    },
    {
        "name": "gaeloideachas.ie",
        "fn": lambda: scrape_headings("https://gaeloideachas.ie/foluntais/", "gaeloideachas.ie", "Education", "https://gaeloideachas.ie"),
    },
    {
        "name": "coimisineir.ie",
        "fn": lambda: scrape_headings("https://www.coimisineir.ie/", "coimisineir.ie", "Irish Language", "https://www.coimisineir.ie"),
    },
    {
        "name": "tuairisc.ie",
        "fn": lambda: scrape_headings("https://tuairisc.ie/foluntais/", "tuairisc.ie", "Media", "https://tuairisc.ie"),
    },
    {
        "name": "comhar.ie",
        "fn": lambda: scrape_headings("https://comhar.ie/eolas/foluntas/", "comhar.ie", "Irish Language Publishing", "https://comhar.ie"),
    },
    {
        "name": "gael-linn.ie",
        "fn": lambda: scrape_headings("https://www.gael-linn.ie/en/working-with-us/", "gael-linn.ie", "Irish Language", "https://www.gael-linn.ie"),
    },
    {
        "name": "raidionalife.ie",
        "fn": lambda: scrape_headings("https://www.raidionalife.ie/en/vacancies/", "raidionalife.ie", "Media", "https://www.raidionalife.ie"),
    },
    {
        "name": "comharnaionrai.ie",
        "fn": lambda: scrape_headings("https://www.comharnaionrai.ie/foluntais/", "comharnaionrai.ie", "Irish Language Education", "https://www.comharnaionrai.ie"),
    },
    {
        "name": "europus.ie",
        "fn": lambda: scrape_headings("https://ga-europus.ie/vacancies/", "europus.ie", "Irish Language", "https://ga-europus.ie"),
    },
    {
        "name": "glornangael.ie",
        "fn": lambda: scrape_headings("https://www.glornangael.ie/", "glornangael.ie", "Irish Language Community", "https://www.glornangael.ie"),
    },
    {
        "name": "comhairle-na-gaelscolaiochta",
        "fn": lambda: scrape_headings("https://comahirle-na-gaelscolaiochta.squarespace.com/foluntais-vacancies", "comhairle-na-gaelscolaiochta.com", "Irish Language Education", "https://comahirle-na-gaelscolaiochta.squarespace.com"),
    },

    # Media — Irish language roles specifically
    {
        "name": "tg4.ie",
        "fn": lambda: scrape_headings("https://www.tg4.ie/en/corporate/vacancies/", "tg4.ie", "Media", "https://www.tg4.ie"),
    },
    {
        "name": "rte.ie",
        "fn": lambda: scrape_headings("https://about.rte.ie/working-with-rte/vacancies/", "rte.ie", "Media", "https://about.rte.ie", irish_only=True),
    },

    # Government — filter for Irish language roles
    {
        "name": "oireachtas.ie",
        "fn": lambda: scrape_headings("https://www.oireachtas.ie/en/how-parliament-is-run/houses-of-the-oireachtas-service/careers/", "oireachtas.ie", "Government", "https://www.oireachtas.ie"),
    },
    {
        "name": "garda.ie",
        "fn": lambda: scrape_headings("https://www.garda.ie/en/careers/", "garda.ie", "Public Safety", "https://www.garda.ie", irish_only=True),
    },
    {
        "name": "avondhublackwater.com",
        "fn": lambda: scrape_headings("https://www.avondhublackwater.com/leader-contracts/", "avondhublackwater.com", "Community Development", "https://www.avondhublackwater.com"),
    },

    # Health
    {
        "name": "careerhub.hse.ie",
        "fn": lambda: scrape_headings("https://careerhub.hse.ie/current-vacancies/", "careerhub.hse.ie", "Health", "https://careerhub.hse.ie", irish_only=True),
    },

    # Local Government
    {
        "name": "localgovernmentjobs.ie",
        "fn": lambda: scrape_headings("https://www.localgovernmentjobs.ie/Search/Vacancies?keyword=gaeilge", "localgovernmentjobs.ie", "Local Government", "https://www.localgovernmentjobs.ie", irish_only=True),
    },

    # Education — filter for Irish language
    {
        "name": "educationposts.ie",
        "fn": lambda: scrape_headings("https://www.educationposts.ie/", "educationposts.ie", "Education", "https://www.educationposts.ie", irish_only=True),
    },
    {
        "name": "universityofgalway.ie",
        "fn": lambda: scrape_headings("https://www.universityofgalway.ie/about-us/jobs/", "universityofgalway.ie", "Higher Education", "https://www.universityofgalway.ie", irish_only=True),
    },
    {
        "name": "dcu.ie",
        "fn": lambda: scrape_headings("https://www.dcu.ie/people/jobs", "dcu.ie", "Higher Education", "https://www.dcu.ie", irish_only=True),
    },
    {
        "name": "ucc.ie",
        "fn": lambda: scrape_headings("https://www.ucc.ie/en/hr/vacancies/academic/", "ucc.ie", "Higher Education", "https://www.ucc.ie", irish_only=True),
    },
    {
        "name": "ul.ie",
        "fn": lambda: scrape_headings("https://www.ul.ie/vacancies", "ul.ie", "Higher Education", "https://www.ul.ie", irish_only=True),
    },
    {
        "name": "mic.ul.ie",
        "fn": lambda: scrape_headings("https://www.mic.ul.ie/about-mic/vacancies", "mic.ul.ie", "Higher Education", "https://www.mic.ul.ie", irish_only=True),
    },

    # EU / International
    {
        "name": "eu-careers.europa.eu",
        "fn": lambda: scrape_headings("https://eu-careers.europa.eu/en/job-opportunities/", "eu-careers.europa.eu", "EU Institutions", "https://eu-careers.europa.eu", irish_only=True),
    },
    {
        "name": "ireland.ie (EU Jobs)",
        "fn": lambda: scrape_headings("https://www.ireland.ie/en/eu-jobs/eu-careers-with-the-irish-language/", "ireland.ie", "EU Institutions", "https://www.ireland.ie"),
    },

    # General boards — Irish language keyword filter
    {
        "name": "publicjobs.ie",
        "fn": lambda: scrape_headings("https://publicjobs.ie/en/", "publicjobs.ie", "Public Sector", "https://publicjobs.ie", irish_only=True),
    },
    {
        "name": "jobsireland.ie",
        "fn": lambda: scrape_headings("https://jobsireland.ie/en-US/browse-jobs", "jobsireland.ie", "General", "https://jobsireland.ie", irish_only=True),
    },
]


def scrape_adzuna() -> list:
    app_id  = os.environ.get("ADZUNA_APP_ID", "").strip()
    app_key = os.environ.get("ADZUNA_APP_KEY", "").strip()
    if not app_id or not app_key:
        print("  Adzuna: no keys — skipping")
        return []
    jobs = []
    for term in ["gaeilge", "irish language officer"]:
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
                title        = item.get("title", "")
                redirect_url = item.get("redirect_url", "")
                if not redirect_url or not title:
                    continue
                jobs.append({
                    "title":       title,
                    "description": item.get("description", "")[:300],
                    "county":      item.get("location", {}).get("display_name", ""),
                    "sector":      item.get("category", {}).get("label", ""),
                    "source_url":  redirect_url,
                    "source_name": "adzuna.ie",
                })
            time.sleep(1)
        except Exception as e:
            print(f"  Adzuna error: {e}")
    return jobs


def cleanup_expired_jobs():
    today = date.today().isoformat()
    try:
        requests.delete(
            f"{SUPABASE_URL}/rest/v1/jobs",
            headers=HEADERS_SB,
            params={"is_aggregated": "eq.true", "closing_date": f"lt.{today}"},
            timeout=10,
        )
        print("Cleanup done")
    except Exception as e:
        print(f"Cleanup error: {e}")


def main():
    print(f"poist.ie job scraper v6 — {len(SOURCES) + 1} sources\n")

    all_jobs = []
    for source in SOURCES:
        print(f"Checking {source['name']}...")
        all_jobs += source["fn"]()

    print(f"\nChecking Adzuna...")
    all_jobs += scrape_adzuna()

    print(f"\nFound {len(all_jobs)} genuine job listings\n")

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

    print(f"\nFinal: {inserted} inserted, {skipped} skipped")
    cleanup_expired_jobs()


if __name__ == "__main__":
    main()