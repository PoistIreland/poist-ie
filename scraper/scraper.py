"""
poist.ie — Irish Language Job Scraper v7
- Fetches real descriptions + closing dates from job detail pages
- Default closing_date 60 days out so jobs eventually expire
- Cleanup removes expired jobs AND stale aggregated jobs (90+ days old)
"""

import os
import re
import requests
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
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
    "translator", "ateangaire", "interpreter", "gaelscoil", "gaelcholáiste",
]

COUNTIES = [
    "Dublin", "Cork", "Galway", "Kerry", "Limerick", "Waterford", "Mayo",
    "Donegal", "Tipperary", "Clare", "Wicklow", "Meath", "Kildare", "Louth",
    "Wexford", "Kilkenny", "Cavan", "Roscommon", "Offaly", "Laois", "Sligo",
    "Westmeath", "Leitrim", "Longford", "Monaghan", "Carlow",
]

# Matches "closing date", "deadline", "by", etc. followed by a date
DATE_RE = re.compile(
    r'(?:clos(?:ing)?\s*(?:date)?|deadline|applications?\s*(?:by|close|before)|submit\s*by)'
    r'[^0-9]{0,50}'
    r'(\d{1,2})[\/\-\.\s]+(\w+)[\/\-\.\s]+(\d{2,4})',
    re.IGNORECASE,
)
MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    "ean": 1, "feabh": 2, "mart": 3, "aibre": 4, "bealt": 5, "meith": 6,
    "iul": 7, "lun": 8, "mfom": 9, "dfom": 10, "samh": 11, "noll": 12,
}


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


def parse_closing_date(text: str) -> str | None:
    """Extract closing date from page text. Returns ISO string or None."""
    match = DATE_RE.search(text)
    if not match:
        return None
    day_str, mon_str, yr_str = match.group(1), match.group(2), match.group(3)
    try:
        if mon_str.isdigit():
            mon_num = int(mon_str)
        else:
            mon_num = MONTH_MAP.get(mon_str.lower()[:4])
            if not mon_num:
                return None
        yr = int(yr_str) if len(yr_str) == 4 else 2000 + int(yr_str)
        dt = datetime(yr, mon_num, int(day_str))
        if dt.date() <= date.today():
            return None  # Already past — skip
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OverflowError):
        return None


def detect_county(text: str) -> str:
    for county in COUNTIES:
        if re.search(r"\b" + county + r"\b", text, re.IGNORECASE):
            return county
    return ""


def fetch_job_detail(url: str) -> dict:
    """Visit a job detail page and extract description, county, closing date."""
    result: dict = {"description": "", "county": "", "closing_date": None}
    try:
        resp = requests.get(url, headers=SCRAPE_HEADERS, timeout=15)
        if resp.status_code != 200:
            return result
        soup = BeautifulSoup(resp.text, "html.parser")

        # Strip navigation noise
        for el in soup.find_all(["nav", "footer", "header", "script", "style", "aside"]):
            el.decompose()

        # Try main content selectors in priority order
        desc_text = ""
        for sel in [
            "article", "main", ".job-description", ".vacancy-description",
            ".entry-content", "#content", ".content", ".post-content",
        ]:
            el = soup.select_one(sel)
            if el:
                t = el.get_text(separator=" ", strip=True)
                if len(t) > 150:
                    desc_text = t[:2500]
                    break
        if not desc_text:
            desc_text = soup.get_text(separator=" ", strip=True)[:2500]

        result["description"] = desc_text
        result["county"] = detect_county(desc_text)
        closing = parse_closing_date(soup.get_text())
        if closing:
            result["closing_date"] = closing

        time.sleep(1)
    except Exception as e:
        print(f"    detail fetch error ({url[:70]}): {e}")
    return result


def insert_job(job: dict) -> bool:
    default_closing = (date.today() + timedelta(days=60)).isoformat()
    payload = {
        "title":        job["title"],
        "description":  job.get("description", ""),
        "county":       job.get("county", ""),
        "sector":       job.get("sector", ""),
        "status":       "approved",
        "is_aggregated": True,
        "source_url":   job["source_url"],
        "source_name":  job["source_name"],
        "closing_date": job.get("closing_date") or default_closing,
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
    Extract job listings by targeting h2/h3/h4 headings with links.
    Falls back to article/li containers if no headings found.
    Set irish_only=True for general sites that need keyword filtering.
    """
    jobs = []
    try:
        resp = requests.get(url, headers=SCRAPE_HEADERS, timeout=15)
        print(f"  {source_name}: status {resp.status_code}")
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        seen: set = set()

        def make_job(title, href):
            if not href or href.startswith("#") or "javascript:" in href:
                return None
            full_url = href if href.startswith("http") else (
                f"{base_url.rstrip('/')}{href if href.startswith('/') else '/' + href}"
            )
            if full_url in seen:
                return None
            seen.add(full_url)
            if not is_valid_job_title(title):
                return None
            if irish_only and not any(kw in title.lower() for kw in IRISH_LANGUAGE_KEYWORDS):
                return None
            return {
                "title": title,
                "description": "",
                "county": "", "sector": sector,
                "source_url": full_url, "source_name": source_name,
            }

        # Strategy 1: h2/h3/h4 headings with links
        for tag in soup.find_all(["h2", "h3", "h4"]):
            link = tag.find("a", href=True)
            if not link:
                continue
            j = make_job(tag.get_text(strip=True), link.get("href", ""))
            if j:
                jobs.append(j)

        # Strategy 2: standalone <a> tags that look like job titles (not inside headings)
        if not jobs:
            for link in soup.find_all("a", href=True):
                if link.find_parent(["h2", "h3", "h4"]):
                    continue
                title = link.get_text(strip=True)
                j = make_job(title, link.get("href", ""))
                if j:
                    jobs.append(j)

        # Strategy 3: article / list-item containers
        if not jobs:
            for container in soup.select(
                "article, li.job, li.post, .job-listing, .vacancy-item, .job-item, .careers-item"
            ):
                link = container.find("a", href=True)
                if not link:
                    continue
                title = link.get_text(strip=True)
                j = make_job(title, link.get("href", ""))
                if j:
                    jobs.append(j)

        print(f"  {source_name}: {len(jobs)} job titles found")
        time.sleep(2)

    except Exception as e:
        print(f"  {source_name} error: {e}")

    return jobs


# ── SOURCES ───────────────────────────────────────────────────────────────────
# Focuses on plain-HTML sites that BeautifulSoup can reliably read.
# JS-rendered job boards (publicjobs.ie, educationposts.ie, etc.) are omitted
# because they return an empty shell without a headless browser.

SOURCES = [
    # ── Irish Language Specific ─────────────────────────────────────────────
    {
        "name": "sceal.ie",
        "fn": lambda: scrape_headings(
            "https://sceal.ie/foluntais/", "sceal.ie",
            "Irish Language Community", "https://sceal.ie"),
    },
    {
        "name": "forasnagaeilge.ie",
        "fn": lambda: scrape_headings(
            "https://www.forasnagaeilge.ie/about-foras-na-gaeilge/vacancies/?lang=en",
            "forasnagaeilge.ie", "Irish Language", "https://www.forasnagaeilge.ie"),
    },
    {
        "name": "udaras.ie",
        "fn": lambda: scrape_headings(
            "https://www.udaras.ie/foluntais/", "udaras.ie",
            "Gaeltacht Development", "https://www.udaras.ie"),
    },
    {
        "name": "gaeloideachas.ie",
        "fn": lambda: scrape_headings(
            "https://gaeloideachas.ie/foluntais/", "gaeloideachas.ie",
            "Education", "https://gaeloideachas.ie"),
    },
    {
        "name": "tuairisc.ie",
        "fn": lambda: scrape_headings(
            "https://tuairisc.ie/foluntais/", "tuairisc.ie",
            "Media", "https://tuairisc.ie"),
    },
    {
        "name": "comhar.ie",
        "fn": lambda: scrape_headings(
            "https://comhar.ie/eolas/foluntas/", "comhar.ie",
            "Irish Language Publishing", "https://comhar.ie"),
    },
    {
        "name": "gael-linn.ie",
        "fn": lambda: scrape_headings(
            "https://www.gael-linn.ie/en/working-with-us/", "gael-linn.ie",
            "Irish Language", "https://www.gael-linn.ie"),
    },
    {
        "name": "raidionalife.ie",
        "fn": lambda: scrape_headings(
            "https://www.raidionalife.ie/en/vacancies/", "raidionalife.ie",
            "Media", "https://www.raidionalife.ie"),
    },
    {
        "name": "comharnaionrai.ie",
        "fn": lambda: scrape_headings(
            "https://www.comharnaionrai.ie/foluntais/", "comharnaionrai.ie",
            "Irish Language Education", "https://www.comharnaionrai.ie"),
    },
    {
        "name": "glornangael.ie",
        "fn": lambda: scrape_headings(
            "https://www.glornangael.ie/", "glornangael.ie",
            "Irish Language Community", "https://www.glornangael.ie"),
    },
    {
        "name": "cnag.ie",
        "fn": lambda: scrape_headings(
            "https://cnag.ie/en/about-us/vacancies/", "cnag.ie",
            "Irish Language Promotion", "https://cnag.ie"),
    },
    {
        "name": "gaelport.com",
        "fn": lambda: scrape_headings(
            "https://www.gaelport.com/default.aspx?treeid=37", "gaelport.com",
            "Irish Language", "https://www.gaelport.com"),
    },
    {
        "name": "gaelchultur.com",
        "fn": lambda: scrape_headings(
            "https://gaelchultur.com/eolas-faoi-ghaelchultur/foluntais/", "gaelchultur.com",
            "Irish Language Education", "https://gaelchultur.com"),
    },
    {
        "name": "coimisineir.ie",
        "fn": lambda: scrape_headings(
            "https://www.coimisineir.ie/", "coimisineir.ie",
            "Irish Language", "https://www.coimisineir.ie"),
    },
    {
        "name": "europus.ie",
        "fn": lambda: scrape_headings(
            "https://ga-europus.ie/vacancies/", "europus.ie",
            "Irish Language", "https://ga-europus.ie"),
    },
    {
        "name": "avondhublackwater.com",
        "fn": lambda: scrape_headings(
            "https://www.avondhublackwater.com/leader-contracts/",
            "avondhublackwater.com", "Community Development",
            "https://www.avondhublackwater.com"),
    },

    # ── Media ────────────────────────────────────────────────────────────────
    {
        "name": "tg4.ie",
        "fn": lambda: scrape_headings(
            "https://www.tg4.ie/en/corporate/vacancies/", "tg4.ie",
            "Media", "https://www.tg4.ie"),
    },
    {
        "name": "rte.ie",
        "fn": lambda: scrape_headings(
            "https://about.rte.ie/working-with-rte/vacancies/", "rte.ie",
            "Media", "https://about.rte.ie", irish_only=True),
    },

    # ── Government & Public Sector (plain-HTML or reliable) ──────────────────
    {
        "name": "oireachtas.ie",
        "fn": lambda: scrape_headings(
            "https://www.oireachtas.ie/en/how-parliament-is-run/houses-of-the-oireachtas-service/careers/",
            "oireachtas.ie", "Government", "https://www.oireachtas.ie"),
    },
    {
        "name": "localgovernmentjobs.ie",
        "fn": lambda: scrape_headings(
            "https://www.localgovernmentjobs.ie/Search/Vacancies?keyword=gaeilge",
            "localgovernmentjobs.ie", "Local Government",
            "https://www.localgovernmentjobs.ie", irish_only=True),
    },

    # ── Higher Education ─────────────────────────────────────────────────────
    {
        "name": "universityofgalway.ie",
        "fn": lambda: scrape_headings(
            "https://www.universityofgalway.ie/about-us/jobs/", "universityofgalway.ie",
            "Higher Education", "https://www.universityofgalway.ie", irish_only=True),
    },
    {
        "name": "ucc.ie",
        "fn": lambda: scrape_headings(
            "https://www.ucc.ie/en/hr/vacancies/academic/", "ucc.ie",
            "Higher Education", "https://www.ucc.ie", irish_only=True),
    },

    # ── EU & International ───────────────────────────────────────────────────
    {
        "name": "ireland.ie (EU Jobs)",
        "fn": lambda: scrape_headings(
            "https://www.ireland.ie/en/eu-jobs/eu-careers-with-the-irish-language/",
            "ireland.ie", "EU Institutions", "https://www.ireland.ie"),
    },
]


def scrape_adzuna() -> list:
    app_id  = os.environ.get("ADZUNA_APP_ID", "").strip()
    app_key = os.environ.get("ADZUNA_APP_KEY", "").strip()
    if not app_id or not app_key:
        print("  Adzuna: no keys — skipping")
        return []
    jobs = []
    terms = [
        "gaeilge", "irish language officer", "múinteoir gaeilge",
        "aistritheoir", "gaeltacht", "gaelscoil",
    ]
    for term in terms:
        try:
            resp = requests.get(
                "https://api.adzuna.com/v1/api/jobs/ie/search/1",
                params={
                    "app_id": app_id, "app_key": app_key,
                    "what": term, "results_per_page": 20,
                },
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
                    "description": item.get("description", "")[:2000],
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
    today     = date.today().isoformat()
    stale_cut = (date.today() - timedelta(days=90)).isoformat()
    try:
        # Delete aggregated jobs whose closing date has passed
        requests.delete(
            f"{SUPABASE_URL}/rest/v1/jobs",
            headers=HEADERS_SB,
            params={"is_aggregated": "eq.true", "closing_date": f"lt.{today}"},
            timeout=10,
        )
        # Safety net: delete very old aggregated jobs with no closing date
        requests.delete(
            f"{SUPABASE_URL}/rest/v1/jobs",
            headers=HEADERS_SB,
            params={
                "is_aggregated": "eq.true",
                "closing_date":  "is.null",
                "created_at":    f"lt.{stale_cut}",
            },
            timeout=10,
        )
        print("Cleanup done")
    except Exception as e:
        print(f"Cleanup error: {e}")


def main():
    print(f"poist.ie job scraper v7 — {len(SOURCES) + 1} sources\n")

    all_jobs: list = []
    for source in SOURCES:
        print(f"Checking {source['name']}...")
        all_jobs += source["fn"]()

    print(f"\nChecking Adzuna...")
    all_jobs += scrape_adzuna()

    print(f"\nFound {len(all_jobs)} candidate listings\n")

    inserted = skipped = 0
    for job in all_jobs:
        if not job.get("source_url") or not job.get("title"):
            continue
        if url_already_exists(job["source_url"]):
            skipped += 1
            continue
        # Fetch real description + closing date from the job's own page
        if not job.get("description"):
            print(f"  Fetching detail: {job['source_url'][:70]}")
            details = fetch_job_detail(job["source_url"])
            for k, v in details.items():
                if v:
                    job[k] = v
        if insert_job(job):
            inserted += 1
        time.sleep(0.3)

    print(f"\nFinal: {inserted} inserted, {skipped} skipped (already in DB)")
    cleanup_expired_jobs()


if __name__ == "__main__":
    main()
