"""
poist.ie — Irish Language Job Scraper v8
Custom per-site scrapers for accuracy.
Removes sites that consistently return junk.
"""

import os
import re
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
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IE,en;q=0.9,ga;q=0.8",
}

IRISH_COUNTIES = [
    "Antrim","Armagh","Carlow","Cavan","Clare","Cork","Derry","Donegal",
    "Down","Dublin","Fermanagh","Galway","Kerry","Kildare","Kilkenny",
    "Laois","Leitrim","Limerick","Longford","Louth","Mayo","Meath",
    "Monaghan","Offaly","Roscommon","Sligo","Tipperary","Tyrone",
    "Waterford","Westmeath","Wexford","Wicklow",
]

# Words that are never job titles
NEVER_JOB = [
    "tuilleadh eolais", "read more", "níos mó", "click here",
    "folúntais", "foluntais", "vacancies", "careers", "jobs",
    "home", "about", "contact", "search", "privacy", "cookies",
    "meet the team", "work experience", "how we recruit", "why work with us",
    "next page", "previous page", "older entries",
    "register vacancy", "cláraigh folúntais",
]

# Must contain one of these to be considered a job title
JOB_INDICATORS = [
    "oifigeach", "officer", "manager", "bainisteoir", "múinteoir", "teacher",
    "oibrí", "worker", "riarthóir", "administrator", "cléireach", "clerk",
    "comhairleoir", "advisor", "analyst", "anailísí", "innealtóir", "engineer",
    "stiúrthóir", "director", "cúntóir", "assistant", "speisialtóir", "specialist",
    "ceannaire", "leader", "coordinator", "comhordaitheoir", "planner", "pleanálaí",
    "editor", "eagarthóir", "journalist", "iriseoir", "producer", "léiritheoir",
    "translator", "aistritheoir", "interpreter", "ateangaire",
    "researcher", "taighdeoir", "developer", "forbróir",
    "technician", "teicneoir", "nurse", "altra", "therapist", "teiripeoir",
    "príomh", "head of", "ceann", "láithreoir", "presenter", "craoltóir",
    "tuairisceoir", "reporter", "post i", "post in", "folúntas", "foluntas",
    "vacancy", "ceapachán", "appointment", "intéirneacht", "internship",
    "comórtas", "competition", "clerical", "executive", "feidhmiúchán",
    "naíonra", "naíolann", "cúramóir", "caretaker", "glantóir", "cleaner",
    "ionadaí", "representative", "tacaíocht", "support",
]


def is_job_title(title: str) -> bool:
    t = title.strip().lower()
    if len(t) < 8 or len(t) > 200:
        return False
    if any(bad.lower() == t for bad in NEVER_JOB):
        return False
    if any(bad.lower() in t for bad in ["@", "http", "www.", ".ie", ".com"]):
        return False
    # For Irish language sites, accept if contains a job indicator
    if any(ind.lower() in t for ind in JOB_INDICATORS):
        return True
    return False


def extract_county(text: str) -> str:
    for county in IRISH_COUNTIES:
        if county.lower() in text.lower():
            return county
    return ""


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
        "org_name":      job.get("org_name", ""),
        "salary":        job.get("salary", "POA"),
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
        print(f"  FAILED ({resp.status_code}): {job['title']} -- {resp.text[:120]}")
        return False


def get_soup(url: str, source_name: str):
    """Fetch a URL and return BeautifulSoup or None."""
    try:
        resp = requests.get(url, headers=SCRAPE_HEADERS, timeout=15, allow_redirects=True)
        print(f"  {source_name}: status {resp.status_code}")
        if resp.status_code != 200:
            return None
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        print(f"  {source_name} error: {e}")
        return None


def extract_jobs_from_headings(soup, source_name: str, sector: str,
                                base_url: str, org_name: str,
                                require_job_indicator: bool = True) -> list:
    """Extract jobs from h2/h3/h4 headings that contain links."""
    jobs = []
    seen = set()

    for tag in soup.find_all(["h2", "h3", "h4"]):
        link = tag.find("a", href=True)
        if not link:
            continue
        title = tag.get_text(strip=True)
        href  = link.get("href", "")
        if not href or href.startswith("#") or "javascript:" in href or "@" in href:
            continue
        full_url = href if href.startswith("http") else f"{base_url.rstrip('/')}{href if href.startswith('/') else '/' + href}"
        if full_url in seen:
            continue
        seen.add(full_url)
        if require_job_indicator and not is_job_title(title):
            continue
        county = extract_county(title)
        parent = tag.find_parent(["article", "li", "div", "section"])
        if parent and not county:
            county = extract_county(parent.get_text())
        salary = ""
        if parent:
            m = re.search(r'€[\d,]+(?:\s*[-–]\s*€[\d,]+)?', parent.get_text())
            if m:
                salary = m.group(0)
        jobs.append({
            "title":       title,
            "description": f"Role advertised on {source_name}. Visit the original listing for full details.",
            "county":      county,
            "sector":      sector,
            "org_name":    org_name,
            "salary":      salary or "POA",
            "source_url":  full_url,
            "source_name": source_name,
        })

    return jobs


# ── CUSTOM SCRAPERS ───────────────────────────────────────────────────────────

def scrape_sceal():
    """sceal.ie — dedicated Irish language jobs board."""
    soup = get_soup("https://sceal.ie/foluntais/", "sceal.ie")
    if not soup:
        return []
    jobs = extract_jobs_from_headings(soup, "sceal.ie", "Irish Language Community",
                                       "https://sceal.ie", "Scéal", require_job_indicator=True)
    print(f"  sceal.ie: {len(jobs)} jobs")
    return jobs


def scrape_tuairisc():
    """
    tuairisc.ie — Irish language news site with a foluntais section.
    Only accept links that go to /foluntais/ subpages, not news articles.
    """
    soup = get_soup("https://tuairisc.ie/foluntais/", "tuairisc.ie")
    if not soup:
        return []
    jobs = []
    seen = set()
    for tag in soup.find_all(["h2", "h3", "h4"]):
        link = tag.find("a", href=True)
        if not link:
            continue
        title = tag.get_text(strip=True)
        href  = link.get("href", "")
        # Only accept links that are job posts — must go to /foluntais/ path
        if "/foluntais/" not in href and "job" not in href.lower() and "post" not in href.lower():
            continue
        if not is_job_title(title):
            continue
        full_url = href if href.startswith("http") else f"https://tuairisc.ie{href}"
        if full_url in seen:
            continue
        seen.add(full_url)
        jobs.append({
            "title":       title,
            "description": "Role advertised on Tuairisc.ie.",
            "county":      extract_county(title),
            "sector":      "Media",
            "org_name":    "Tuairisc.ie",
            "salary":      "POA",
            "source_url":  full_url,
            "source_name": "tuairisc.ie",
        })
    print(f"  tuairisc.ie: {len(jobs)} jobs")
    return jobs


def scrape_tg4():
    """TG4 vacancies — Irish language broadcaster."""
    soup = get_soup("https://www.tg4.ie/en/corporate/vacancies/", "tg4.ie")
    if not soup:
        return []
    jobs = extract_jobs_from_headings(soup, "tg4.ie", "Media",
                                       "https://www.tg4.ie", "TG4", require_job_indicator=True)
    print(f"  tg4.ie: {len(jobs)} jobs")
    return jobs


def scrape_foras():
    """Foras na Gaeilge vacancies."""
    soup = get_soup("https://www.forasnagaeilge.ie/about-foras-na-gaeilge/vacancies/?lang=en", "forasnagaeilge.ie")
    if not soup:
        return []
    jobs = extract_jobs_from_headings(soup, "forasnagaeilge.ie", "Irish Language",
                                       "https://www.forasnagaeilge.ie", "Foras na Gaeilge",
                                       require_job_indicator=True)
    print(f"  forasnagaeilge.ie: {len(jobs)} jobs")
    return jobs


def scrape_udaras():
    """
    Údarás na Gaeltachta — try both vacancy pages with new URLs.
    """
    jobs = []
    urls = [
        "https://udaras.ie/en/training-employment/vacancies/cliantchomhlachtai-an-udarais-sa-ghaeltacht/",
        "https://udaras.ie/en/training-employment/vacancies/poist-udaras-na-gaeltachta/",
    ]
    for url in urls:
        soup = get_soup(url, "udaras.ie")
        if not soup:
            continue
        found = extract_jobs_from_headings(soup, "udaras.ie", "Gaeltacht Development",
                                            "https://udaras.ie", "Údarás na Gaeltachta",
                                            require_job_indicator=True)
        # Also check for PDF links — udaras posts jobs as downloadable PDFs
        seen_urls = {j["source_url"] for j in found}
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            if ".pdf" in href.lower() and len(text) > 8 and href not in seen_urls:
                full_url = href if href.startswith("http") else f"https://udaras.ie{href}"
                if is_job_title(text) or any(ind in text.lower() for ind in ["folúntas", "post", "vacancy"]):
                    found.append({
                        "title":       text,
                        "description": "Role advertised by Údarás na Gaeltachta. PDF listing.",
                        "county":      "Galway",
                        "sector":      "Gaeltacht Development",
                        "org_name":    "Údarás na Gaeltachta",
                        "salary":      "POA",
                        "source_url":  full_url,
                        "source_name": "udaras.ie",
                    })
                    seen_urls.add(href)
        jobs += found
    print(f"  udaras.ie: {len(jobs)} jobs total")
    return jobs


def scrape_gaeloideachas():
    """Gaeloideachas — Irish medium education jobs."""
    soup = get_soup("https://gaeloideachas.ie/foluntais/", "gaeloideachas.ie")
    if not soup:
        return []
    jobs = extract_jobs_from_headings(soup, "gaeloideachas.ie", "Education",
                                       "https://gaeloideachas.ie", "Gaeloideachas",
                                       require_job_indicator=True)
    print(f"  gaeloideachas.ie: {len(jobs)} jobs")
    return jobs


def scrape_comharnaionrai():
    """Comhar Naíonraí na Gaeltachta — Irish language childcare."""
    soup = get_soup("https://www.comharnaionrai.ie/foluntais/", "comharnaionrai.ie")
    if not soup:
        return []
    jobs = extract_jobs_from_headings(soup, "comharnaionrai.ie", "Irish Language Education",
                                       "https://www.comharnaionrai.ie", "Comhar Naíonraí na Gaeltachta",
                                       require_job_indicator=True)
    print(f"  comharnaionrai.ie: {len(jobs)} jobs")
    return jobs


def scrape_cnag():
    """Conradh na Gaeilge vacancies."""
    soup = get_soup("https://cnag.ie/en/info/conradh-na-gaeilge/facts-and-figures.html?view=article&id=1463:vacancy&catid=13",
                    "cnag.ie")
    if not soup:
        return []
    jobs = extract_jobs_from_headings(soup, "cnag.ie", "Irish Language Promotion",
                                       "https://cnag.ie", "Conradh na Gaeilge",
                                       require_job_indicator=True)
    print(f"  cnag.ie: {len(jobs)} jobs")
    return jobs


def scrape_coimisineir():
    """Oifig an Choimisinéara Teanga."""
    soup = get_soup("https://www.coimisineir.ie/index.cfm?page=vacancies", "coimisineir.ie")
    if not soup:
        return []
    jobs = extract_jobs_from_headings(soup, "coimisineir.ie", "Irish Language",
                                       "https://www.coimisineir.ie", "Oifig an Choimisinéara Teanga",
                                       require_job_indicator=True)
    print(f"  coimisineir.ie: {len(jobs)} jobs")
    return jobs


def scrape_comhar():
    """Comhar — Irish language publisher."""
    soup = get_soup("https://comhar.ie/eolas/foluntas/", "comhar.ie")
    if not soup:
        return []
    jobs = extract_jobs_from_headings(soup, "comhar.ie", "Irish Language Publishing",
                                       "https://comhar.ie", "Comhar",
                                       require_job_indicator=True)
    print(f"  comhar.ie: {len(jobs)} jobs")
    return jobs


def scrape_rte():
    """RTÉ — Irish national broadcaster. Filter for Irish language roles only."""
    soup = get_soup("https://about.rte.ie/working-with-rte/vacancies/", "rte.ie")
    if not soup:
        return []
    all_jobs = extract_jobs_from_headings(soup, "rte.ie", "Media",
                                           "https://about.rte.ie", "RTÉ",
                                           require_job_indicator=True)
    IRISH_KW = ["gaeilge", "irish", "gaeltacht", "raidió na gaeltachta", "nuacht"]
    jobs = [j for j in all_jobs if any(kw in j["title"].lower() for kw in IRISH_KW)]
    print(f"  rte.ie: {len(jobs)} Irish language jobs (from {len(all_jobs)} total)")
    return jobs


def scrape_raidionalife():
    """Raidió na Life."""
    soup = get_soup("https://www.raidionalife.ie/en/vacancies/", "raidionalife.ie")
    if not soup:
        return []
    jobs = extract_jobs_from_headings(soup, "raidionalife.ie", "Media",
                                       "https://www.raidionalife.ie", "Raidió na Life",
                                       require_job_indicator=True)
    print(f"  raidionalife.ie: {len(jobs)} jobs")
    return jobs


def scrape_oireachtas():
    """
    Houses of the Oireachtas — only accept links that go to actual job/recruitment pages.
    """
    soup = get_soup("https://www.oireachtas.ie/en/how-parliament-is-run/houses-of-the-oireachtas-service/careers/",
                    "oireachtas.ie")
    if not soup:
        return []
    jobs = []
    seen = set()
    for tag in soup.find_all(["h2", "h3", "h4"]):
        link = tag.find("a", href=True)
        if not link:
            continue
        title = tag.get_text(strip=True)
        href  = link.get("href", "")
        # Only accept links to actual vacancy/recruitment pages
        if not any(w in href.lower() for w in ["vacanc", "recruit", "job", "competition", "compet"]):
            continue
        if not is_job_title(title):
            continue
        full_url = href if href.startswith("http") else f"https://www.oireachtas.ie{href}"
        if full_url in seen:
            continue
        seen.add(full_url)
        jobs.append({
            "title":       title,
            "description": "Role with Houses of the Oireachtas.",
            "county":      "Dublin",
            "sector":      "Government",
            "org_name":    "Houses of the Oireachtas",
            "salary":      "POA",
            "source_url":  full_url,
            "source_name": "oireachtas.ie",
        })
    print(f"  oireachtas.ie: {len(jobs)} jobs")
    return jobs


def scrape_localgovt():
    """LocalGovernmentJobs.ie — search for gaeilge roles."""
    jobs = []
    for term in ["gaeilge", "irish+language"]:
        soup = get_soup(f"https://www.localgovernmentjobs.ie/Search/Vacancies?keyword={term}",
                        "localgovernmentjobs.ie")
        if not soup:
            continue
        IRISH_KW = ["gaeilge", "irish language", "oifigeach gaeilge", "irish officer"]
        all_jobs = extract_jobs_from_headings(soup, "localgovernmentjobs.ie", "Local Government",
                                               "https://www.localgovernmentjobs.ie", "",
                                               require_job_indicator=True)
        jobs += [j for j in all_jobs if any(kw in j["title"].lower() for kw in IRISH_KW)]
        time.sleep(1)
    print(f"  localgovernmentjobs.ie: {len(jobs)} jobs")
    return jobs


def scrape_hse():
    """HSE Career Hub — filter for Irish language roles."""
    soup = get_soup("https://careerhub.hse.ie/current-vacancies/", "careerhub.hse.ie")
    if not soup:
        return []
    IRISH_KW = ["gaeilge", "irish", "gaeltacht", "bilingual"]
    all_jobs = extract_jobs_from_headings(soup, "careerhub.hse.ie", "Health",
                                           "https://careerhub.hse.ie", "HSE",
                                           require_job_indicator=True)
    jobs = [j for j in all_jobs if any(kw in j["title"].lower() for kw in IRISH_KW)]
    print(f"  careerhub.hse.ie: {len(jobs)} Irish language jobs")
    return jobs


def scrape_publicjobs():
    """PublicJobs.ie — search for Irish language roles."""
    soup = get_soup("https://publicjobs.ie/en/", "publicjobs.ie")
    if not soup:
        return []
    IRISH_KW = ["gaeilge", "irish language", "irish stream", "gaeltacht"]
    all_jobs = extract_jobs_from_headings(soup, "publicjobs.ie", "Public Sector",
                                           "https://publicjobs.ie", "Public Appointments Service",
                                           require_job_indicator=True)
    jobs = [j for j in all_jobs if any(kw in j["title"].lower() for kw in IRISH_KW)]
    print(f"  publicjobs.ie: {len(jobs)} Irish language jobs")
    return jobs


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
                loc    = item.get("location", {}).get("display_name", "")
                county = extract_county(loc)
                salary_min = item.get("salary_min")
                salary_max = item.get("salary_max")
                if salary_min and salary_max:
                    salary = f"€{int(salary_min):,} – €{int(salary_max):,}"
                elif salary_min:
                    salary = f"From €{int(salary_min):,}"
                else:
                    salary = "POA"
                jobs.append({
                    "title":       title,
                    "description": item.get("description", "")[:300],
                    "county":      county,
                    "sector":      item.get("category", {}).get("label", ""),
                    "org_name":    item.get("company", {}).get("display_name", ""),
                    "salary":      salary,
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
    print("poist.ie job scraper v8 — custom per-site scrapers\n")

    scrapers = [
        ("sceal.ie",               scrape_sceal),
        ("tuairisc.ie",            scrape_tuairisc),
        ("tg4.ie",                 scrape_tg4),
        ("foras na gaeilge",       scrape_foras),
        ("udaras.ie",              scrape_udaras),
        ("gaeloideachas.ie",       scrape_gaeloideachas),
        ("comhar naionrai",        scrape_comharnaionrai),
        ("cnag.ie",                scrape_cnag),
        ("coimisineir.ie",         scrape_coimisineir),
        ("comhar.ie",              scrape_comhar),
        ("rte.ie",                 scrape_rte),
        ("raidionalife.ie",        scrape_raidionalife),
        ("oireachtas.ie",          scrape_oireachtas),
        ("localgovernmentjobs.ie", scrape_localgovt),
        ("careerhub.hse.ie",       scrape_hse),
        ("publicjobs.ie",          scrape_publicjobs),
        ("adzuna",                 scrape_adzuna),
    ]

    all_jobs = []
    for name, fn in scrapers:
        print(f"Checking {name}...")
        all_jobs += fn()
        time.sleep(1)

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