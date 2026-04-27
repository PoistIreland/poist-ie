"""
poist.ie — Nightly Irish Language Job Scraper
Enhanced v2: granular Irish location extraction (Carraroe, Spiddal, etc.)
Runs via GitHub Actions at 23:00 UTC daily.
"""

from __future__ import annotations
import os, re, time, logging
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
ADZUNA_APP_ID = os.environ.get("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY", "")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    "User-Agent": "poist.ie/2.0 Job Aggregator (hello@poist.ie) — Irish language jobs platform"
}

# ─────────────────────────────────────────────────────────────────────────────
# GRANULAR IRISH LOCATION DATABASE
# Maps specific towns/villages/townlands → (display_name, county, lat, lng)
# Prioritises Gaeltacht areas and Irish-language place names.
# ─────────────────────────────────────────────────────────────────────────────
IRISH_PLACES = {
    # ── Galway / Connemara Gaeltacht ──────────────────────────────────────────
    "carraroe":         ("Carraroe, Co. Galway",      "Galway",   53.2690, -9.5930),
    "an cheathrú rua":  ("Carraroe, Co. Galway",      "Galway",   53.2690, -9.5930),
    "an cheathrú":      ("Carraroe, Co. Galway",      "Galway",   53.2690, -9.5930),
    "spiddal":          ("Spiddal, Co. Galway",        "Galway",   53.2440, -9.3090),
    "an spidéal":       ("Spiddal, Co. Galway",        "Galway",   53.2440, -9.3090),
    "salthill":         ("Salthill, Co. Galway",       "Galway",   53.2590, -9.0780),
    "connemara":        ("Connemara, Co. Galway",      "Galway",   53.4500, -9.9000),
    "conamara":         ("Connemara, Co. Galway",      "Galway",   53.4500, -9.9000),
    "clifden":          ("Clifden, Co. Galway",        "Galway",   53.4880, -10.0200),
    "an clochán":       ("Clifden, Co. Galway",        "Galway",   53.4880, -10.0200),
    "oughterard":       ("Oughterard, Co. Galway",     "Galway",   53.4270, -9.3220),
    "uachtar ard":      ("Oughterard, Co. Galway",     "Galway",   53.4270, -9.3220),
    "tuam":             ("Tuam, Co. Galway",            "Galway",   53.5150, -8.8560),
    "loughrea":         ("Loughrea, Co. Galway",       "Galway",   53.1970, -8.5650),
    "ballinasloe":      ("Ballinasloe, Co. Galway",    "Galway",   53.3310, -8.2200),
    "gort":             ("Gort, Co. Galway",            "Galway",   53.0670, -8.8200),
    "rosmuc":           ("Rosmuc, Co. Galway",         "Galway",   53.3910, -9.5430),
    "ros muc":          ("Rosmuc, Co. Galway",         "Galway",   53.3910, -9.5430),
    "lettermore":       ("Lettermore, Co. Galway",     "Galway",   53.3140, -9.5900),
    "leitir mór":       ("Lettermore, Co. Galway",     "Galway",   53.3140, -9.5900),
    "carna":            ("Carna, Co. Galway",           "Galway",   53.3260, -9.8560),
    "kilronan":         ("Kilronan, Inis Mór",          "Galway",   53.1240, -9.6590),
    "cill rónáin":      ("Kilronan, Inis Mór",          "Galway",   53.1240, -9.6590),
    "inis mór":         ("Inis Mór, Co. Galway",       "Galway",   53.1240, -9.6590),
    "inis oírr":        ("Inis Oírr, Co. Galway",      "Galway",   53.0660, -9.5070),
    "inis meáin":       ("Inis Meáin, Co. Galway",     "Galway",   53.0940, -9.5730),
    "galway":           ("Galway City",                 "Galway",   53.2707, -9.0568),
    "gaillimh":         ("Galway City",                 "Galway",   53.2707, -9.0568),

    # ── Donegal Gaeltacht ────────────────────────────────────────────────────
    "gaoth dobhair":    ("Gaoth Dobhair, Co. Donegal",  "Donegal",  55.0560, -8.2550),
    "gweedore":         ("Gaoth Dobhair, Co. Donegal",  "Donegal",  55.0560, -8.2550),
    "falcarragh":       ("Falcarragh, Co. Donegal",     "Donegal",  55.1350, -8.1050),
    "an fál carrach":   ("Falcarragh, Co. Donegal",     "Donegal",  55.1350, -8.1050),
    "dungloe":          ("Dungloe, Co. Donegal",        "Donegal",  54.9470, -8.3620),
    "an clochán liath": ("Dungloe, Co. Donegal",        "Donegal",  54.9470, -8.3620),
    "glenties":         ("Glenties, Co. Donegal",       "Donegal",  54.7940, -8.2760),
    "na gleannta":      ("Glenties, Co. Donegal",       "Donegal",  54.7940, -8.2760),
    "ardara":           ("Ardara, Co. Donegal",         "Donegal",  54.7640, -8.4090),
    "killybegs":        ("Killybegs, Co. Donegal",      "Donegal",  54.6380, -8.4490),
    "na cealla beaga":  ("Killybegs, Co. Donegal",      "Donegal",  54.6380, -8.4490),
    "letterkenny":      ("Letterkenny, Co. Donegal",    "Donegal",  54.9500, -7.7330),
    "leitir ceanainn":  ("Letterkenny, Co. Donegal",    "Donegal",  54.9500, -7.7330),
    "donegal":          ("Donegal Town",                "Donegal",  54.6540, -8.1100),
    "na doirí beaga":   ("Doirí Beaga, Co. Donegal",   "Donegal",  55.0200, -8.1200),
    "derrybeg":         ("Derrybeg, Co. Donegal",       "Donegal",  55.0200, -8.1200),
    "bunbeg":           ("Bunbeg, Co. Donegal",         "Donegal",  55.0600, -8.2900),
    "bun beag":         ("Bunbeg, Co. Donegal",         "Donegal",  55.0600, -8.2900),
    "gortahork":        ("Gortahork, Co. Donegal",      "Donegal",  55.1200, -8.1500),
    "gort an choirce":  ("Gortahork, Co. Donegal",      "Donegal",  55.1200, -8.1500),

    # ── Kerry Gaeltacht ──────────────────────────────────────────────────────
    "dingle":           ("Dingle, Co. Kerry",           "Kerry",    52.1409, -10.2680),
    "an daingean":      ("Dingle, Co. Kerry",           "Kerry",    52.1409, -10.2680),
    "daingean uí chúis":("Dingle, Co. Kerry",           "Kerry",    52.1409, -10.2680),
    "tralee":           ("Tralee, Co. Kerry",           "Kerry",    52.2715, -9.7006),
    "trá lí":           ("Tralee, Co. Kerry",           "Kerry",    52.2715, -9.7006),
    "killarney":        ("Killarney, Co. Kerry",        "Kerry",    52.0600, -9.5000),
    "cill airne":       ("Killarney, Co. Kerry",        "Kerry",    52.0600, -9.5000),
    "ventry":           ("Ventry, Co. Kerry",           "Kerry",    52.1170, -10.3440),
    "ceann trá":        ("Ventry, Co. Kerry",           "Kerry",    52.1170, -10.3440),
    "ballyferriter":    ("Ballyferriter, Co. Kerry",    "Kerry",    52.1650, -10.4060),
    "baile an fheirtéaraigh": ("Ballyferriter, Co. Kerry", "Kerry", 52.1650, -10.4060),
    "dunquin":          ("Dunquin, Co. Kerry",          "Kerry",    52.1250, -10.4630),
    "dún chaoin":       ("Dunquin, Co. Kerry",          "Kerry",    52.1250, -10.4630),

    # ── Mayo Gaeltacht ───────────────────────────────────────────────────────
    "achill":           ("Achill, Co. Mayo",            "Mayo",     53.9400, -10.0500),
    "acaill":           ("Achill, Co. Mayo",            "Mayo",     53.9400, -10.0500),
    "westport":         ("Westport, Co. Mayo",          "Mayo",     53.8000, -9.5200),
    "cathair na mart":  ("Westport, Co. Mayo",          "Mayo",     53.8000, -9.5200),
    "castlebar":        ("Castlebar, Co. Mayo",         "Mayo",     53.8600, -9.3000),
    "caisleán an bharraigh": ("Castlebar, Co. Mayo",   "Mayo",     53.8600, -9.3000),
    "belmullet":        ("Belmullet, Co. Mayo",         "Mayo",     54.2240, -9.9890),
    "béal an mhuirthead":("Belmullet, Co. Mayo",        "Mayo",     54.2240, -9.9890),
    "tourmakeady":      ("Tourmakeady, Co. Mayo",       "Mayo",     53.6220, -9.4220),
    "tuar mhic éadaigh":("Tourmakeady, Co. Mayo",       "Mayo",     53.6220, -9.4220),

    # ── Meath / Ráth Cairn ──────────────────────────────────────────────────
    "ráth cairn":       ("Ráth Cairn, Co. Meath",      "Meath",    53.5680, -6.9610),
    "rathcairn":        ("Ráth Cairn, Co. Meath",      "Meath",    53.5680, -6.9610),
    "baile ghib":       ("Baile Ghib, Co. Meath",      "Meath",    53.5600, -6.9800),

    # ── Major cities ─────────────────────────────────────────────────────────
    "dublin":           ("Dublin",                      "Dublin",   53.3498, -6.2603),
    "baile átha cliath":("Dublin",                      "Dublin",   53.3498, -6.2603),
    "cork":             ("Cork City",                   "Cork",     51.8985, -8.4756),
    "corcaigh":         ("Cork City",                   "Cork",     51.8985, -8.4756),
    "limerick":         ("Limerick City",               "Limerick", 52.6638, -8.6267),
    "luimneach":        ("Limerick City",               "Limerick", 52.6638, -8.6267),
    "waterford":        ("Waterford City",              "Waterford",52.2566, -7.1221),
    "port láirge":      ("Waterford City",              "Waterford",52.2566, -7.1221),
    "kilkenny":         ("Kilkenny City",               "Kilkenny", 52.6541, -7.2448),
    "cill chainnigh":   ("Kilkenny City",               "Kilkenny", 52.6541, -7.2448),
    "sligo":            ("Sligo Town",                  "Sligo",    54.2766, -8.4761),
    "sligeach":         ("Sligo Town",                  "Sligo",    54.2766, -8.4761),
    "wexford":          ("Wexford Town",                "Wexford",  52.3369, -6.4633),
    "loch garman":      ("Wexford Town",                "Wexford",  52.3369, -6.4633),
    "drogheda":         ("Drogheda, Co. Louth",        "Louth",    53.7182, -6.3563),
    "dundalk":          ("Dundalk, Co. Louth",          "Louth",    54.0042, -6.4074),
    "athlone":          ("Athlone, Co. Westmeath",     "Westmeath",53.4235, -7.9401),
    "áth luain":        ("Athlone, Co. Westmeath",     "Westmeath",53.4235, -7.9401),
    "ennis":            ("Ennis, Co. Clare",            "Clare",    52.8433, -8.9820),
    "inis":             ("Ennis, Co. Clare",            "Clare",    52.8433, -8.9820),
}

# County-level fallback coordinates
COUNTY_COORDS = {
    "Dublin": (53.3498, -6.2603), "Cork": (51.8985, -8.4756),
    "Galway": (53.2707, -9.0568), "Kerry": (52.1545, -9.5669),
    "Limerick": (52.6638, -8.6267), "Tipperary": (52.4735, -8.1619),
    "Waterford": (52.2566, -7.1221), "Kilkenny": (52.6541, -7.2448),
    "Wexford": (52.3369, -6.4633), "Wicklow": (52.9808, -6.0440),
    "Meath": (53.6055, -6.6564), "Louth": (53.9235, -6.4887),
    "Kildare": (53.1561, -6.9096), "Laois": (52.9943, -7.3320),
    "Carlow": (52.8369, -6.9315), "Offaly": (53.2357, -7.7122),
    "Westmeath": (53.5345, -7.4653), "Longford": (53.7272, -7.7940),
    "Roscommon": (53.6279, -8.1918), "Sligo": (54.2766, -8.4761),
    "Mayo": (53.8496, -9.3004), "Leitrim": (54.1244, -8.0001),
    "Cavan": (53.9897, -7.3633), "Monaghan": (54.2491, -6.9688),
    "Donegal": (54.9558, -7.7342),
    "Antrim": (54.7178, -6.2072), "Armagh": (54.3503, -6.6528),
    "Down": (54.3281, -5.9386), "Fermanagh": (54.3440, -7.6307),
    "Tyrone": (54.5991, -7.2989), "Derry": (54.9966, -7.3086),
}

# ─────────────────────────────────────────────────────────────────────────────
# LOCATION EXTRACTION — granular Irish places first, Nominatim fallback
# ─────────────────────────────────────────────────────────────────────────────

def normalize_text(text: str) -> str:
    """Lowercase, remove punctuation noise, normalise whitespace."""
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[,;|•–—·]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_location_from_text(text: str) -> dict | None:
    """
    Scan text for known Irish place names (longest match wins).
    Returns dict with location, county, lat, lng — or None.
    """
    norm = normalize_text(text)
    # Sort by length descending so "an cheathrú rua" matches before "rua"
    for key in sorted(IRISH_PLACES.keys(), key=len, reverse=True):
        if key in norm:
            display, county, lat, lng = IRISH_PLACES[key]
            return {"location": display, "county": county, "lat": lat, "lng": lng}
    return None


def geocode_nominatim(location_text: str, retry: int = 3) -> tuple[float | None, float | None, str | None]:
    """
    Geocode a free-text location string using Nominatim (OpenStreetMap).
    Returns (lat, lng, county) or (None, None, None).
    """
    if not location_text:
        return None, None, None

    # First try our local database (zero network cost, instant)
    result = extract_location_from_text(location_text)
    if result:
        return result["lat"], result["lng"], result["county"]

    # Fallback to Nominatim API
    for attempt in range(retry):
        try:
            query = location_text if "ireland" in location_text.lower() else location_text + ", Ireland"
            resp = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": query, "format": "json", "addressdetails": 1,
                        "limit": 1, "countrycodes": "ie"},
                headers=HEADERS,
                timeout=10,
            )
            results = resp.json()
            if not results:
                return None, None, None
            r = results[0]
            addr = r.get("address", {})
            county_raw = addr.get("county", addr.get("state", ""))
            # Strip "County " prefix and normalise
            county = re.sub(r"^county\s+", "", county_raw, flags=re.IGNORECASE).strip().title()
            return float(r["lat"]), float(r["lon"]), county or None
        except Exception as e:
            log.warning(f"Nominatim error (attempt {attempt+1}): {e}")
            time.sleep(1)

    return None, None, None


def parse_location_field(raw_location: str) -> dict:
    """
    Given a raw location string from a job listing, return a dict with:
    location (human-readable), county, lat, lng.
    Tries local database first, then Nominatim.
    Rate-limiting: 1 request/second to Nominatim (their ToS requirement).
    """
    if not raw_location:
        return {"location": None, "county": None, "lat": None, "lng": None}

    # 1. Try local database
    result = extract_location_from_text(raw_location)
    if result:
        log.info(f"  ✅ Local match: '{raw_location}' → {result['location']}")
        return result

    # 2. Nominatim (with 1-second rate limit)
    time.sleep(1)
    lat, lng, county = geocode_nominatim(raw_location)

    # 3. County-level coord fallback
    if county and not lat and county in COUNTY_COORDS:
        lat, lng = COUNTY_COORDS[county]

    # Build a clean display name
    location_display = raw_location.strip().title() if raw_location else None

    log.info(f"  📍 Nominatim: '{raw_location}' → county={county}, lat={lat}, lng={lng}")
    return {"location": location_display, "county": county, "lat": lat, "lng": lng}


# ─────────────────────────────────────────────────────────────────────────────
# JOB TYPE INFERENCE
# ─────────────────────────────────────────────────────────────────────────────

def infer_job_type(text: str) -> str:
    t = (text or "").lower()
    if any(w in t for w in ["lánaimseartha", "full-time", "full time", "permanent"]):
        return "full_time"
    if any(w in t for w in ["páirtaimseartha", "part-time", "part time"]):
        return "part_time"
    if any(w in t for w in ["conradh", "contract", "fixed term", "fixed-term"]):
        return "contract"
    if any(w in t for w in ["sealadach", "temporary", "temp "]):
        return "temporary"
    return "full_time"


# ─────────────────────────────────────────────────────────────────────────────
# DEDUPLICATION & INSERTION
# ─────────────────────────────────────────────────────────────────────────────

def get_existing_urls() -> set:
    try:
        result = sb.from_("jobs").select("source_url").execute()
        return {r["source_url"] for r in (result.data or []) if r.get("source_url")}
    except Exception as e:
        log.error(f"Failed to fetch existing URLs: {e}")
        return set()


def insert_job(job: dict, existing_urls: set) -> bool:
    """Insert a job if not already present. Returns True if inserted."""
    url = job.get("source_url")
    if url and url in existing_urls:
        return False

    # Resolve location
    raw_loc = job.pop("_raw_location", None) or job.get("location") or job.get("county") or ""
    loc_data = parse_location_field(raw_loc)

    job.setdefault("location", loc_data["location"])
    job.setdefault("county", loc_data["county"])
    job.setdefault("lat", loc_data["lat"])
    job.setdefault("lng", loc_data["lng"])
    job.setdefault("status", "pending")
    job.setdefault("is_aggregated", True)

    try:
        sb.from_("jobs").insert(job).execute()
        log.info(f"  ✅ Inserted: {job.get('title')} @ {job.get('location') or job.get('county')}")
        if url:
            existing_urls.add(url)
        return True
    except Exception as e:
        log.error(f"  ❌ Insert failed for {job.get('title')}: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPER FUNCTIONS — each site
# ─────────────────────────────────────────────────────────────────────────────

IRISH_JOB_KEYWORDS = re.compile(
    r"irish|gaeilge|gaeltacht|teanga|oifigeach|gaelic|language officer|irish language",
    re.IGNORECASE,
)


def is_irish_job(title: str, desc: str = "") -> bool:
    return bool(IRISH_JOB_KEYWORDS.search(title or "") or IRISH_JOB_KEYWORDS.search(desc or ""))


def scrape_generic(url: str, source_name: str, existing_urls: set) -> int:
    """Generic heading-based scraper for simple job listing pages."""
    inserted = 0
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(resp.text, "html.parser")
        links = soup.find_all("a", href=True)
        for link in links:
            title = link.get_text(strip=True)
            if len(title) < 10 or not is_irish_job(title):
                continue
            href = link["href"]
            if not href.startswith("http"):
                href = urljoin(url, href)

            # Try to get location from surrounding text
            parent_text = link.parent.get_text(" ", strip=True) if link.parent else ""
            loc_result = extract_location_from_text(parent_text) or {}

            job = {
                "title": title,
                "source_url": href,
                "source_name": source_name,
                "job_type": infer_job_type(title + " " + parent_text),
                "_raw_location": loc_result.get("location") or "",
                "location": loc_result.get("location"),
                "county": loc_result.get("county"),
                "lat": loc_result.get("lat"),
                "lng": loc_result.get("lng"),
            }
            inserted += insert_job(job, existing_urls)
    except Exception as e:
        log.error(f"Error scraping {source_name}: {e}")
    return inserted


def scrape_publicjobs(existing_urls: set) -> int:
    """publicjobs.ie — Irish language roles."""
    inserted = 0
    try:
        resp = requests.get(
            "https://www.publicjobs.ie/en/search?keyword=irish+language&pageNumber=1",
            headers=HEADERS, timeout=15
        )
        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.select(".job-title a, h3 a, h2 a"):
            title = item.get_text(strip=True)
            if not title or not is_irish_job(title):
                continue
            href = item.get("href", "")
            if not href.startswith("http"):
                href = "https://www.publicjobs.ie" + href

            parent = item.find_parent(class_=re.compile(r"job|listing|result"))
            parent_text = parent.get_text(" ", strip=True) if parent else ""
            loc_result = extract_location_from_text(parent_text) or {}

            job = {
                "title": title,
                "source_url": href,
                "source_name": "publicjobs.ie",
                "job_type": infer_job_type(parent_text),
                "_raw_location": parent_text,
                "location": loc_result.get("location"),
                "county": loc_result.get("county"),
                "lat": loc_result.get("lat"),
                "lng": loc_result.get("lng"),
            }
            inserted += insert_job(job, existing_urls)
    except Exception as e:
        log.error(f"publicjobs.ie error: {e}")
    return inserted


def scrape_udaras(existing_urls: set) -> int:
    """Údarás na Gaeltachta — main Gaeltacht employer."""
    return scrape_generic("https://www.udaras.ie/fostaíocht/", "Údarás na Gaeltachta", existing_urls)


def scrape_tg4(existing_urls: set) -> int:
    return scrape_generic("https://www.tg4.ie/ga/about/jobs/", "TG4", existing_urls)


def scrape_foras(existing_urls: set) -> int:
    return scrape_generic("https://www.forasnagaeilge.ie/eolas/foluntais/", "Foras na Gaeilge", existing_urls)


def scrape_sceal(existing_urls: set) -> int:
    return scrape_generic("https://sceal.ie/jobs", "Scéal", existing_urls)


def scrape_tuairisc(existing_urls: set) -> int:
    return scrape_generic("https://tuairisc.ie/foluntais/", "Tuairisc.ie", existing_urls)


def scrape_gaeloideachas(existing_urls: set) -> int:
    return scrape_generic("https://www.gaeloideachas.ie/i-am/a-job-seeker/", "Gaeloideachas", existing_urls)


def scrape_comhar_naionrai(existing_urls: set) -> int:
    return scrape_generic("https://www.comharnaionrai.ie/eolas/foluntais/", "Comhar Naíonraí na Gaeltachta", existing_urls)


def scrape_cnag(existing_urls: set) -> int:
    return scrape_generic("https://www.cnag.ie/ga/foilseachain/foluntais.html", "CNAG", existing_urls)


def scrape_coimisineir(existing_urls: set) -> int:
    return scrape_generic("https://www.coimisineir.ie/", "Coimisinéir na Gaeilge", existing_urls)


def scrape_comhar(existing_urls: set) -> int:
    return scrape_generic("https://comhar.ie/foluntais/", "Comhar", existing_urls)


def scrape_rte(existing_urls: set) -> int:
    return scrape_generic("https://www.rte.ie/about/en/jobs/", "RTÉ", existing_urls)


def scrape_raidio_na_life(existing_urls: set) -> int:
    return scrape_generic("https://raidionalife.ie/foluntais/", "Raidió na Life", existing_urls)


def scrape_oireachtas(existing_urls: set) -> int:
    return scrape_generic("https://www.oireachtas.ie/en/about/recruitment-hr/vacancies/", "Oireachtas", existing_urls)


def scrape_local_gov(existing_urls: set) -> int:
    return scrape_generic(
        "https://www.localgovernmentjobs.ie/Jobs/Index?keyword=irish+language",
        "localgovernmentjobs.ie", existing_urls
    )


def scrape_hse(existing_urls: set) -> int:
    return scrape_generic(
        "https://careerhub.hse.ie/candidates/jobs/search/?keyword=irish",
        "HSE Career Hub", existing_urls
    )


def scrape_adzuna(existing_urls: set) -> int:
    """Adzuna API — free tier, 1,000 calls/day."""
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        log.info("Adzuna: credentials not set, skipping.")
        return 0
    inserted = 0
    try:
        resp = requests.get(
            "https://api.adzuna.com/v1/api/jobs/ie/search/1",
            params={
                "app_id": ADZUNA_APP_ID,
                "app_key": ADZUNA_APP_KEY,
                "what": "irish language",
                "results_per_page": 50,
            },
            headers=HEADERS,
            timeout=15,
        )
        data = resp.json()
        for item in data.get("results", []):
            title = item.get("title", "")
            if not is_irish_job(title, item.get("description", "")):
                continue
            raw_loc = item.get("location", {}).get("display_name", "")
            loc_result = extract_location_from_text(raw_loc) or {}
            job = {
                "title": title,
                "source_url": item.get("redirect_url", ""),
                "source_name": "Adzuna",
                "org_name": item.get("company", {}).get("display_name"),
                "salary": _adzuna_salary(item),
                "job_type": infer_job_type(title),
                "_raw_location": raw_loc,
                "location": loc_result.get("location") or raw_loc or None,
                "county": loc_result.get("county"),
                "lat": loc_result.get("lat"),
                "lng": loc_result.get("lng"),
            }
            inserted += insert_job(job, existing_urls)
    except Exception as e:
        log.error(f"Adzuna error: {e}")
    return inserted


def _adzuna_salary(item: dict) -> str | None:
    mn = item.get("salary_min")
    mx = item.get("salary_max")
    if mn and mx:
        return f"€{int(mn):,}–€{int(mx):,}"
    if mn:
        return f"€{int(mn):,}+"
    return None


# ─────────────────────────────────────────────────────────────────────────────
# EXPIRED JOB CLEANUP
# ─────────────────────────────────────────────────────────────────────────────

def cleanup_expired() -> int:
    """Mark aggregated jobs older than 60 days as closed."""
    from datetime import datetime, timedelta, timezone
    cutoff = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
    try:
        result = sb.from_("jobs").update({"status": "closed"}).eq("is_aggregated", True).eq("status", "approved").lt("created_at", cutoff).execute()
        count = len(result.data or [])
        log.info(f"Closed {count} expired aggregated jobs.")
        return count
    except Exception as e:
        log.error(f"Cleanup error: {e}")
        return 0


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("═══ poist.ie Job Scraper v2 — starting ═══")
    existing_urls = get_existing_urls()
    log.info(f"Found {len(existing_urls)} existing job URLs in database.")

    scrapers = [
        ("sceal.ie",                scrape_sceal),
        ("tuairisc.ie",             scrape_tuairisc),
        ("TG4",                     scrape_tg4),
        ("Foras na Gaeilge",        scrape_foras),
        ("Údarás na Gaeltachta",    scrape_udaras),
        ("Gaeloideachas",           scrape_gaeloideachas),
        ("Comhar Naíonraí",         scrape_comhar_naionrai),
        ("CNAG",                    scrape_cnag),
        ("Coimisinéir",             scrape_coimisineir),
        ("Comhar",                  scrape_comhar),
        ("RTÉ",                     scrape_rte),
        ("Raidió na Life",          scrape_raidio_na_life),
        ("Oireachtas",              scrape_oireachtas),
        ("localgovernmentjobs.ie",  scrape_local_gov),
        ("HSE Career Hub",          scrape_hse),
        ("publicjobs.ie",           scrape_publicjobs),
        ("Adzuna",                  scrape_adzuna),
    ]

    total = 0
    for name, fn in scrapers:
        log.info(f"── Scraping {name}…")
        try:
            n = fn(existing_urls)
            log.info(f"   → {n} new jobs inserted from {name}")
            total += n
        except Exception as e:
            log.error(f"   ✗ {name} failed: {e}")

    log.info("── Cleanup expired jobs…")
    cleanup_expired()

    log.info(f"═══ Done. {total} new jobs inserted total. ═══")


if __name__ == "__main__":
    main()