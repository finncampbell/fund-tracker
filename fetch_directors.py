#!/usr/bin/env python3
import os
import json
import logging
import requests
import pandas as pd
from datetime import datetime
from rate_limiter import enforce_rate_limit, record_call

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
API_BASE        = 'https://api.company-information.service.gov.uk/company'
CH_KEY          = os.getenv('CH_API_KEY')
RELEVANT_CSV    = 'assets/data/relevant_companies.csv'
DIRECTORS_JSON  = 'assets/data/directors.json'
LOG_FILE        = 'director_fetch.log'

# ─── LOGGING SETUP ───────────────────────────────────────────────────────────────
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)

def load_relevant_numbers():
    df = pd.read_csv(RELEVANT_CSV, dtype=str)
    # ensure we only get non-null company numbers
    return set(df['Company Number'].dropna())

def load_existing_map():
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON, 'r') as f:
            return json.load(f)
    return {}

def fetch_one(number):
    enforce_rate_limit()
    url    = f"{API_BASE}/{number}/officers"
    params = {'register_view': 'true'}
    try:
        resp = requests.get(url, auth=(CH_KEY, ''), params=params, timeout=10)
        resp.raise_for_status()
        record_call()
        items = resp.json().get('items', [])
    except Exception as e:
        log.warning(f"{number}: fetch error: {e}")
        return None

    # Include both directors and members
    ROLES = {'director', 'member'}

    # Prefer active; otherwise fall back to any in ROLES
    active = [
        off for off in items
        if off.get('officer_role') in ROLES and off.get('resigned_on') is None
    ]
    chosen = active or [
        off for off in items
        if off.get('officer_role') in ROLES
    ]

    directors = []
    for off in chosen:
        # Format date of birth
        dob = off.get('date_of_birth') or {}
        y, m = dob.get('year'), dob.get('month')
        if y and m:
            dob_str = f"{y}-{int(m):02d}"
        elif y:
            dob_str = str(y)
        else:
            dob_str = ''

        directors.append({
            'title':            off.get('name'),
            'appointment':      off.get('snippet') or '',
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
    pending  = [num for num in relevant if num not in existing]

    if not pending:
        log.info("No new companies to fetch directors for.")
        # Always rewrite JSON so it exists
        with open(DIRECTORS_JSON, 'w') as f:
            json.dump(existing, f, separators=(',',':'))
        log.info(f"Wrote directors.json with {len(existing)} existing entries")
        return

    log.info(f"{len(pending)} pending companies to fetch")
    updated = False

    for num in pending:
        dirs = fetch_one(num)
        if dirs is None:
            continue
        existing[num] = dirs
        log.info(f"Fetched {len(dirs)} director(s) for {num}")
        updated = True

    # Write out the merged map (old + new)
    with open(DIRECTORS_JSON, 'w') as f:
        json.dump(existing, f, separators=(',',':'))
    log.info(f"Wrote directors.json with {len(existing)} companies")

if __name__ == '__main__':
    main()
