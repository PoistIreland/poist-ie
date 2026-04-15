"""
poist.ie — Irish Language Job Scraper v2
Uses direct API endpoints and RSS feeds instead of HTML scraping,
so it works regardless of JavaScript rendering.
"""

import os
import requests
import xml.etree.ElementTree as ET
from datetime import date
import time

# ── Supabase config ───────────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
HEADERS_SB   = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

IRISH_KEYWORDS = [
    "gaeilge", "irish language", "líofa", "as gaeilge",
    "gaeltacht", "irish speaker", "fluent irish", "native irish",
    "inniúlacht sa ghaeilge", "labhairt na gaeilge",
]

SCRAPE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; poist.ie-bot/1.0; +https://poist.ie)"
}


def is_irish_language_job(title: str, description: str = "") -> bool:
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
        "cefr_required": job.get("cefr_required", ""),
        "job_type":      job.get("job_type", ""),
        "closing_date":  job.get("closing_date"),
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


def scrape_publicjobs_rss() -> list:
    jobs = []
    for term in ["gaeilge", "irish+language"]:
        try:
            url  = f"https://www.publicjobs.ie/en/rss?q={term}"
            resp = requests.get(url, headers=SCRAPE_HEADERS, timeout=15)
            print(f"  publicjobs RSS ({term}): status {resp.status_code}")
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
            for item in root.findall(".//item"):
                title = item.findtext("title", "")
                link  = item.findtext("link", "")
                desc  = item.findtext("description", "")
                if not title or not link:
                    continue
                if not is_irish_language_job(title, desc):
                    continue
                jobs.append({
                    "title": title,
                    "description": "Irish language role on publicjobs.ie.",
                    "county": "",
                    "sector": "Public Sector",
                    "source_url": link,
                    "source_name": "publicjobs.ie",
                })
            print(f"  publicjobs RSS ({term}): {len(jobs)} matching jobs so far")
            time.sleep(2)
        except Exception as e:
            print(f"  publicjobs RSS error ({term}): {e}")
    return jobs


def scrape_adzuna() -> list:
    app_id  = os.environ.get("ADZUNA_APP_ID", "").strip()
    app_key = os.environ.get("ADZUNA_APP_KEY", "").strip()
    if not app_id or not app_key:
        print("  Adzuna: no keys set -- skipping")
        return []
    jobs = []
    for term in ["gaeilge", "irish language"]:
        try:
            url = "https://api.adzuna.com/v1/api/jobs/ie/search/1"
            params = {
                "app_id": app_id, "app_key": app_key,
                "what": term, "results_per_page": 20,
            }
            resp = requests.get(url, params=params, timeout=15)
            for item in resp.json().get("results", []):
                title = item.get("title", "")
                if not is_irish_language_job(title, item.get("description", "")):
                    continue
                redirect_url = item.get("redirect_url", "")
                if not redirect_url:
                    continue
                jobs.append({
                    "title": title,
                    "description": "Irish language role via Adzuna.",
                    "county": item.get("location", {}).get("display_name", ""),
                    "sector": item.get("category", {}).get("label", ""),
                    "source_url": redirect_url,
                    "source_name": "adzuna.ie",
                })
            time.sleep(1)
        except Exception as e:
            print(f"  Adzuna error ({term}): {e}")
    return jobs


def scrape_rss_feed(url: str, source_name: str, sector: str) -> list:
    jobs = []
    try:
        resp = requests.get(url, headers=SCRAPE_HEADERS, timeout=15)
        print(f"  {source_name}: status {resp.status_code}")
        if resp.status_code != 200:
            return []
        root = ET.fromstring(resp.content)
        count = 0
        for item in root.findall(".//item"):
            title = item.findtext("title", "")
            link  = item.findtext("link", "")
            desc  = item.findtext("description", "")
            if not title or not link:
                continue
            count += 1
            if not is_irish_language_job(title, desc):
                continue
            jobs.append({
                "title": title,
                "description": f"Irish language role via {source_name}.",
                "county": "",
                "sector": sector,
                "source_url": link,
                "source_name": source_name,
            })
        print(f"  {source_name}: {count} total items, {len(jobs)} matching")
        time.sleep(2)
    except Exception as e:
        print(f"  {source_name} error: {e}")
    return jobs


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


def main():
    print("poist.ie job scraper v2 starting...\n")

    all_jobs = []

    print("Checking publicjobs.ie RSS...")
    all_jobs += scrape_publicjobs_rss()

    print("Checking Adzuna...")
    all_jobs += scrape_adzuna()

    print("Checking HSE Careers RSS...")
    all_jobs += scrape_rss_feed(
        "https://careers.hse.ie/rss/jobs.rss", "HSE Careers", "Health"
    )

    print("Checking eTenders...")
    all_jobs += scrape_rss_feed(
        "https://www.etenders.gov.ie/epps/cft/downloadRssFeed.do?feedType=CURRENT_TENDERS_FEED",
        "etenders.gov.ie", "Public Sector"
    )

    print(f"\nFound {len(all_jobs)} potential Irish language jobs\n")

    inserted = 0
    skipped  = 0

    for job in all_jobs:
        if not job.get("source_url"):
            skipped += 1
            continue
        if url_already_exists(job["source_url"]):
            print(f"  Already exists: {job['title']}")
            skipped += 1
            continue
        if insert_job(job):
            inserted += 1
        time.sleep(0.5)

    print(f"\nDone -- {inserted} inserted, {skipped} skipped")
    cleanup_expired_jobs()


if __name__ == "__main__":
    main()