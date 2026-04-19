"""
poist.ie — Irish Language Job Scraper v9
Custom per-site scrapers + job type inference + geocoding prep.
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

IRISH_PLACE_TO_COUNTY = {
    "corcaigh": "Cork", "luimneach": "Limerick", "gaillimh": "Galway",
    "baile atha cliath": "Dublin", "baile átha cliath": "Dublin",
    "port láirge": "Waterford", "cill áirne": "Kerry", "loch an iúir": "Donegal",
    "an daingean": "Kerry", "conamara": "Galway", "connemara": "Galway",
    "an cheathrú rua": "Galway", "an spidéal": "Galway", "ros muc": "Galway",
    "carna": "Galway", "an clochán": "Galway", "gaoth dobhair": "Donegal",
    "leitir ceanainn": "Donegal", "falcarragh": "Donegal", "gweedore": "Donegal",
    "belmullet": "Mayo", "maigh eo": "Mayo", "sligeach": "Sligo",
    "liatroim": "Leitrim", "an clár": "Clare", "laois": "Laois",
    "ceatharlach": "Carlow", "an cabhán": "Cavan", "loch garman": "Wexford",
    "muineachán": "Monaghan", "an longfort": "Longford", "an iarmhí": "Westmeath",
    "tiobraid árann": "Tipperary", "ros comáin": "Roscommon",
    "cill dara": "Kildare", "cill chainnigh": "Kilkenny", "an mhi": "Meath",
    "uíbh fhailí": "Offaly",
}

NEVER_JOB = [
    "tuilleadh eolais", "read more", "níos mó", "click here",
    "folúntais", "foluntais", "vacancies", "careers", "jobs",
    "home", "about", "contact", "search", "privacy", "cookies",
    "meet the team", "work experience", "how we recruit", "why work with us",
    "next page", "previous page", "older entries",
    "register vacancy", "cláraigh folúntais",
]

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
    if any(bad in t for bad in ["@", "http", "www.", ".ie", ".com"]):
        return False
    if any(ind.lower() in t for ind in JOB_INDICATORS):
        return True
    return False


def extract_county(text: str) -> str:
    t = text.lower()
    for place, county in IRISH_PLACE_TO_COUNTY.items():
        if place in t:
            return county
    for county in IRISH_COUNTIES:
        if county.lower() in t:
            return county
    return ""


def infer_job_type(title: str, description: str = "") -> str:
    """Infer job_type from Irish and English keywords."""
    text = (title + " " + description).lower()

    part_time_kw  = ["páirtaimseartha", "part-time", "part time", "leath-aimseartha"]
    temporary_kw  = ["sealadach", "temporary", "clúdach", "cover", "maternity cover",
                     "saoire mháithreachais", "ionadaíocht", "ionadaí"]
    contract_kw   = ["conradh", "fixed term", "fixed-term", "ar conradh", "téarma seasta"]
    full_time_kw  = ["lánaimseartha", "full-time", "full time", "lán-aimseartha",
                     "buan", "permanent"]

    for kw in part_time_kw:
        if kw in text: return "part_time"
    for kw in temporary_kw:
        if kw in text: return "temporary"
    for kw in contract_kw:
        if kw in text: return "contract"
    for kw in full_time_kw:
        if kw in text: return "full_time"
    return ""


def geocode(location: str, county: str = "") -> tuple:
    """
    Use Nominatim (OpenStreetMap) to get lat/lng for a location string.
    Returns (lat, lng) or (None, None).
    Free, no API key needed. Rate limit: 1 request/second.
    """
    if not location and not county:
        return None, None
    query = location if location else county
    if county and county not in query:
        query = query + ", " + county + ", Ireland"
    else:
        query = query + ", Ireland"
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query, "format": "json", "limit": 1, "countrycodes": "ie"},
            headers={"User-Agent": "poist.ie-bot/1.0 (hello@poist.ie)"},
            timeout=10,
        )
        results = resp.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception:
        pass
    # Fallback to county centroid
    if county:
        COUNTY_COORDS = {
            "Antrim":[54.72,-6.21],"Armagh":[54.35,-6.65],"Carlow":[52.84,-6.93],
            "Cavan":[53.99,-7.36],"Clare":[52.89,-9.0],"Cork":[51.9,-8.47],
            "Derry":[55.0,-7.31],"Donegal":[54.65,-8.12],"Down":[54.32,-5.95],
            "Dublin":[53.35,-6.26],"Fermanagh":[54.35,-7.63],"Galway":[53.27,-9.06],
            "Kerry":[52.16,-9.57],"Kildare":[53.16,-6.91],"Kilkenny":[52.65,-7.25],
            "Laois":[52.99,-7.33],"Leitrim":[54.0,-8.0],"Limerick":[52.66,-8.63],
            "Longford":[53.73,-7.79],"Louth":[53.92,-6.49],"Mayo":[53.85,-9.3],
            "Meath":[53.61,-6.66],"Monaghan":[54.25,-6.97],"Offaly":[53.27,-7.49],
            "Roscommon":[53.63,-8.19],"Sligo":[54.27,-8.47],"Tipperary":[52.47,-8.16],
            "Tyrone":[54.6,-7.2],"Waterford":[52.26,-7.12],"Westmeath":[53.53,-7.46],
            "Wexford":[52.34,-6.46],"Wicklow":[52.98,-6.44],
        }
        coords = COUNTY_COORDS.get(county)
        if coords:
            return coords[0], coords[1]
    return None, None


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
        "location":      job.get("location", ""),
        "sector":        job.get("sector", ""),
        "org_name":      job.get("org_name", ""),
        "salary":        job.get("salary", "POA"),
        "job_type":      job.get("job_type") or None,
        "lat":           job.get("lat"),
        "lng":           job.get("lng"),
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
        job_type = infer_job_type(title)
        lat, lng = geocode(county, county)
        time.sleep(1)  # Nominatim rate limit: 1 req/sec
        jobs.append({
            "title":       title,
            "description": f"Role advertised on {source_name}. Visit the original listing for full details.",
            "county":      county,
            "location":    county,
            "sector":      sector,
            "org_name":    org_name,
            "salary":      salary or "POA",
            "job_type":    job_type or None,
            "lat":         lat,
            "lng":         lng,
            "source_url":  full_url,
            "source_name": source_name,
        })
    return jobs


# ── SCRAPERS ──────────────────────────────────────────────────────────────────

def scrape_sceal():
    soup = get_soup("https://sceal.ie/foluntais/", "sceal.ie")
    if not soup: return []
    jobs = extract_jobs_from_headings(soup, "sceal.ie", "Irish Language Community",
                                       "https://sceal.ie", "Scéal")
    print(f"  sceal.ie: {len(jobs)} jobs")
    return jobs

def scrape_tuairisc():
    soup = get_soup("https://tuairisc.ie/foluntais/", "tuairisc.ie")
    if not soup: return []
    jobs = []
    seen = set()
    for tag in soup.find_all(["h2", "h3", "h4"]):
        link = tag.find("a", href=True)
        if not link: continue
        title = tag.get_text(strip=True)
        href  = link.get("href", "")
        if "/foluntais/" not in href and "job" not in href.lower():
            continue
        if not is_job_title(title): continue
        full_url = href if href.startswith("http") else f"https://tuairisc.ie{href}"
        if full_url in seen: continue
        seen.add(full_url)
        county = extract_county(title)
        lat, lng = geocode(county, county)
        time.sleep(1)
        jobs.append({
            "title": title, "description": "Role advertised on Tuairisc.ie.",
            "county": county, "location": county, "sector": "Media",
            "org_name": "Tuairisc.ie", "salary": "POA",
            "job_type": infer_job_type(title),
            "lat": lat, "lng": lng,
            "source_url": full_url, "source_name": "tuairisc.ie",
        })
    print(f"  tuairisc.ie: {len(jobs)} jobs")
    return jobs

def scrape_tg4():
    soup = get_soup("https://www.tg4.ie/en/corporate/vacancies/", "tg4.ie")
    if not soup: return []
    jobs = extract_jobs_from_headings(soup, "tg4.ie", "Media", "https://www.tg4.ie", "TG4")
    print(f"  tg4.ie: {len(jobs)} jobs")
    return jobs

def scrape_foras():
    soup = get_soup("https://www.forasnagaeilge.ie/about-foras-na-gaeilge/vacancies/?lang=en", "forasnagaeilge.ie")
    if not soup: return []
    jobs = extract_jobs_from_headings(soup, "forasnagaeilge.ie", "Irish Language",
                                       "https://www.forasnagaeilge.ie", "Foras na Gaeilge")
    print(f"  forasnagaeilge.ie: {len(jobs)} jobs")
    return jobs

def scrape_udaras():
    jobs = []
    for url in [
        "https://udaras.ie/en/training-employment/vacancies/cliantchomhlachtai-an-udarais-sa-ghaeltacht/",
        "https://udaras.ie/en/training-employment/vacancies/poist-udaras-na-gaeltachta/",
    ]:
        soup = get_soup(url, "udaras.ie")
        if not soup: continue
        found = extract_jobs_from_headings(soup, "udaras.ie", "Gaeltacht Development",
                                            "https://udaras.ie", "Údarás na Gaeltachta")
        seen_urls = {j["source_url"] for j in found}
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            if ".pdf" in href.lower() and len(text) > 8 and href not in seen_urls:
                full_url = href if href.startswith("http") else f"https://udaras.ie{href}"
                if is_job_title(text):
                    lat, lng = geocode("Galway", "Galway")
                    time.sleep(1)
                    found.append({
                        "title": text, "description": "Role by Údarás na Gaeltachta. PDF listing.",
                        "county": "Galway", "location": "Galway", "sector": "Gaeltacht Development",
                        "org_name": "Údarás na Gaeltachta", "salary": "POA",
                        "job_type": infer_job_type(text),
                        "lat": lat, "lng": lng,
                        "source_url": full_url, "source_name": "udaras.ie",
                    })
                    seen_urls.add(href)
        jobs += found
    print(f"  udaras.ie: {len(jobs)} jobs")
    return jobs

def scrape_gaeloideachas():
    soup = get_soup("https://gaeloideachas.ie/foluntais/", "gaeloideachas.ie")
    if not soup: return []
    jobs = extract_jobs_from_headings(soup, "gaeloideachas.ie", "Education",
                                       "https://gaeloideachas.ie", "Gaeloideachas")
    print(f"  gaeloideachas.ie: {len(jobs)} jobs")
    return jobs

def scrape_comharnaionrai():
    soup = get_soup("https://www.comharnaionrai.ie/foluntais/", "comharnaionrai.ie")
    if not soup: return []
    jobs = extract_jobs_from_headings(soup, "comharnaionrai.ie", "Irish Language Education",
                                       "https://www.comharnaionrai.ie", "Comhar Naíonraí na Gaeltachta")
    print(f"  comharnaionrai.ie: {len(jobs)} jobs")
    return jobs

def scrape_cnag():
    soup = get_soup("https://cnag.ie/en/info/conradh-na-gaeilge/facts-and-figures.html?view=article&id=1463:vacancy&catid=13", "cnag.ie")
    if not soup: return []
    jobs = extract_jobs_from_headings(soup, "cnag.ie", "Irish Language Promotion",
                                       "https://cnag.ie", "Conradh na Gaeilge")
    print(f"  cnag.ie: {len(jobs)} jobs")
    return jobs

def scrape_coimisineir():
    soup = get_soup("https://www.coimisineir.ie/index.cfm?page=vacancies", "coimisineir.ie")
    if not soup: return []
    jobs = extract_jobs_from_headings(soup, "coimisineir.ie", "Irish Language",
                                       "https://www.coimisineir.ie", "Oifig an Choimisinéara Teanga")
    print(f"  coimisineir.ie: {len(jobs)} jobs")
    return jobs

def scrape_comhar():
    soup = get_soup("https://comhar.ie/eolas/foluntas/", "comhar.ie")
    if not soup: return []
    jobs = extract_jobs_from_headings(soup, "comhar.ie", "Irish Language Publishing",
                                       "https://comhar.ie", "Comhar")
    print(f"  comhar.ie: {len(jobs)} jobs")
    return jobs

def scrape_rte():
    soup = get_soup("https://about.rte.ie/working-with-rte/vacancies/", "rte.ie")
    if not soup: return []
    all_jobs = extract_jobs_from_headings(soup, "rte.ie", "Media", "https://about.rte.ie", "RTÉ")
    IRISH_KW = ["gaeilge", "irish", "gaeltacht", "nuacht"]
    jobs = [j for j in all_jobs if any(kw in j["title"].lower() for kw in IRISH_KW)]
    print(f"  rte.ie: {len(jobs)} Irish language jobs")
    return jobs

def scrape_raidionalife():
    soup = get_soup("https://www.raidionalife.ie/en/vacancies/", "raidionalife.ie")
    if not soup: return []
    jobs = extract_jobs_from_headings(soup, "raidionalife.ie", "Media",
                                       "https://www.raidionalife.ie", "Raidió na Life")
    print(f"  raidionalife.ie: {len(jobs)} jobs")
    return jobs

def scrape_oireachtas():
    soup = get_soup("https://www.oireachtas.ie/en/how-parliament-is-run/houses-of-the-oireachtas-service/careers/", "oireachtas.ie")
    if not soup: return []
    jobs = []
    seen = set()
    for tag in soup.find_all(["h2", "h3", "h4"]):
        link = tag.find("a", href=True)
        if not link: continue
        title = tag.get_text(strip=True)
        href  = link.get("href", "")
        if not any(w in href.lower() for w in ["vacanc", "recruit", "job", "compet"]):
            continue
        if not is_job_title(title): continue
        full_url = href if href.startswith("http") else f"https://www.oireachtas.ie{href}"
        if full_url in seen: continue
        seen.add(full_url)
        lat, lng = geocode("Dublin", "Dublin")
        time.sleep(1)
        jobs.append({
            "title": title, "description": "Role with Houses of the Oireachtas.",
            "county": "Dublin", "location": "Dublin", "sector": "Government",
            "org_name": "Houses of the Oireachtas", "salary": "POA",
            "job_type": infer_job_type(title),
            "lat": lat, "lng": lng,
            "source_url": full_url, "source_name": "oireachtas.ie",
        })
    print(f"  oireachtas.ie: {len(jobs)} jobs")
    return jobs

def scrape_localgovt():
    jobs = []
    for term in ["gaeilge", "irish+language"]:
        soup = get_soup(f"https://www.localgovernmentjobs.ie/Search/Vacancies?keyword={term}", "localgovernmentjobs.ie")
        if not soup: continue
        IRISH_KW = ["gaeilge", "irish language", "oifigeach gaeilge", "irish officer"]
        all_jobs = extract_jobs_from_headings(soup, "localgovernmentjobs.ie", "Local Government",
                                               "https://www.localgovernmentjobs.ie", "")
        jobs += [j for j in all_jobs if any(kw in j["title"].lower() for kw in IRISH_KW)]
        time.sleep(1)
    print(f"  localgovernmentjobs.ie: {len(jobs)} jobs")
    return jobs

def scrape_hse():
    soup = get_soup("https://careerhub.hse.ie/current-vacancies/", "careerhub.hse.ie")
    if not soup: return []
    IRISH_KW = ["gaeilge", "irish", "gaeltacht", "bilingual"]
    all_jobs = extract_jobs_from_headings(soup, "careerhub.hse.ie", "Health",
                                           "https://careerhub.hse.ie", "HSE")
    jobs = [j for j in all_jobs if any(kw in j["title"].lower() for kw in IRISH_KW)]
    print(f"  careerhub.hse.ie: {len(jobs)} jobs")
    return jobs

def scrape_publicjobs():
    soup = get_soup("https://publicjobs.ie/en/", "publicjobs.ie")
    if not soup: return []
    IRISH_KW = ["gaeilge", "irish language", "irish stream", "gaeltacht"]
    all_jobs = extract_jobs_from_headings(soup, "publicjobs.ie", "Public Sector",
                                           "https://publicjobs.ie", "Public Appointments Service")
    jobs = [j for j in all_jobs if any(kw in j["title"].lower() for kw in IRISH_KW)]
    print(f"  publicjobs.ie: {len(jobs)} jobs")
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
                if not redirect_url or not title: continue
                loc    = item.get("location", {}).get("display_name", "")
                county = extract_county(loc)
                lat, lng = geocode(loc, county)
                time.sleep(1)
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
                    "location":    loc,
                    "sector":      item.get("category", {}).get("label", ""),
                    "org_name":    item.get("company", {}).get("display_name", ""),
                    "salary":      salary,
                    "job_type":    infer_job_type(title),
                    "lat":         lat,
                    "lng":         lng,
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
    print("poist.ie job scraper v9 — job type inference + geocoding\n")

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