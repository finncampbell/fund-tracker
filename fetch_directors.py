#!/usr/bin/env python3
import os, json, time, logging, requests, pandas as pd
from datetime import datetime

# ── CONFIG ─────────────────────────────────────────────────────────────────────
API_BASE            = 'https://api.company-information.service.gov.uk/company'
CH_KEY              = os.getenv('CH_API_KEY')
RELEVANT_CSV        = 'assets/data/relevant_companies.csv'
DIRECTORS_JSON      = 'assets/data/directors.json'
LOG_FILE            = 'director_fetch.log'

# Target maximum time for the fetch run (e.g. 30 minutes = 1800s)
MAX_RUNTIME_SECS    = 30 * 60  

# Bounds for per-request delay (in seconds)
MIN_PAUSE           = 0.05   # up to 20 req/sec
MAX_PAUSE           = 1.0    # at most 1 req/sec

# ── LOGGING SETUP ───────────────────────────────────────────────────────────────
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)

def load_relevant_numbers():
    df = pd.read_csv(RELEVANT_CSV, dtype=str)
    return set(df['Company Number'].dropna().tolist())

def load_existing_map():
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON, 'r') as f:
            return json.load(f)
    return {}

def fetch_one(number):
    url    = f"{API_BASE}/{number}/officers"
    params = {'register_view': 'true', 'register_type': 'directors'}
    try:
        resp = requests.get(url, auth=(CH_KEY, ''), params=params, timeout=10)
        resp.raise_for_status()
        # only keep active directors (no resigned_on date)
        items = [
            off for off in resp.json().get('items', [])
            if off.get('resigned_on') is None
        ]
    except Exception as e:
        log.warning(f"{number}: fetch error: {e}")
        return None

    directors = []
    for off in items:
        # format date of birth into YYYY-MM or YYYY
        dob = off.get('date_of_birth') or {}
        year, month = dob.get('year'), dob.get('month')
        if year and month:
            dob_str = f"{year}-{int(month):02d}"
        elif year:
            dob_str = str(year)
        else:
            dob_str = ''

        directors.append({
            'title':            off.get('name'),
            'snippet':          off.get('snippet'),
            'dateOfBirth':      dob_str,
            'appointmentCount': off.get('appointment_count'),
            'selfLink':         off['links'].get('self'),
            'officerRole':      off.get('officer_role'),
            'nationality':      off.get('nationality'),
            'occupation':       off.get('occupation'),
        })
    return directors

def main():
    if not CH_KEY:
        log.error("CH_API_KEY missing")
        return

    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)

    relevant = load_relevant_numbers()
    existing = load_existing_map()

    pending = [num for num in relevant if num not in existing]
    count   = len(pending)
    if count == 0:
        log.info("No new companies to fetch directors for.")
        return

    pause = MAX_RUNTIME_SECS / count
    pause = max(min(pause, MAX_PAUSE), MIN_PAUSE)
    log.info(f"{count} pending companies, using {pause:.2f}s delay per call")

    updated = False
    for num in pending:
        dirs = fetch_one(num)
        if dirs is None:
            continue
        existing[num] = dirs
        log.info(f"Fetched directors for {num} ({len(dirs)} records)")
        updated = True
        time.sleep(pause)

    if updated:
        with open(DIRECTORS_JSON, 'w') as f:
            json.dump(existing, f, separators=(',',':'))
        log.info(f"Wrote directors.json with {len(existing)} companies’ data")

if __name__ == '__main__':
    main()
