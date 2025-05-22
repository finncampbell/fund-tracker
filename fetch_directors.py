#!/usr/bin/env python3
import os, json, time, logging, requests, pandas as pd
from datetime import datetime

# ── CONFIG ─────────────────────────────────────────────────────────────────────
API_BASE       = 'https://api.company-information.service.gov.uk/company'
CH_KEY         = os.getenv('CH_API_KEY')
RELEVANT_CSV   = 'assets/data/relevant_companies.csv'
DIRECTORS_JSON = 'assets/data/directors.json'
LOG_FILE       = 'director_fetch.log'
PAUSE_SECONDS  = 0.2   # throttle between requests

# ── LOGGING SETUP ───────────────────────────────────────────────────────────────
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)

def load_relevant_numbers():
    df = pd.read_csv(RELEVANT_CSV, dtype=str)
    return df['Company Number'].dropna().unique().tolist()

def load_existing_map():
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON, 'r') as f:
            return json.load(f)
    return {}

def fetch_one(number):
    url = f"{API_BASE}/{number}/officers"
    params = {'register_view': 'true', 'register_type': 'directors'}
    try:
        resp = requests.get(url, auth=(CH_KEY, ''), params=params, timeout=10)
        resp.raise_for_status()
        items = resp.json().get('items', [])
    except Exception as e:
        log.warning(f"{number}: fetch error: {e}")
        return None

    directors = []
    for off in items:
        directors.append({
            'title':            off.get('name'),
            'snippet':          off.get('snippet'),
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
    numbers = load_relevant_numbers()
    existing = load_existing_map()
    updated = False

    for num in numbers:
        if num in existing:
            continue
        dirs = fetch_one(num)
        if dirs is None:
            continue
        existing[num] = dirs
        log.info(f"Fetched directors for {num}")
        updated = True
        time.sleep(PAUSE_SECONDS)

    if updated:
        with open(DIRECTORS_JSON, 'w') as f:
            json.dump(existing, f, separators=(',',':'))
        log.info("Wrote directors.json")

if __name__ == '__main__':
    main()
