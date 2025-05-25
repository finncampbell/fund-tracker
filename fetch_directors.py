#!/usr/bin/env python3
"""
fetch_directors.py

- Fetches new companies’ directors
- Writes directly to docs/assets/data/directors.json
- Logs to assets/logs/director_fetch.log
"""

import os
import json
import logging
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from rate_limiter import enforce_rate_limit, record_call

# ─── Config ─────────────────────────────────────────────────────────────────────
API_BASE       = 'https://api.company-information.service.gov.uk/company'
CH_KEY         = os.getenv('CH_API_KEY')
RELEVANT_CSV   = 'docs/assets/data/relevant_companies.csv'
DIRECTORS_JSON = 'docs/assets/data/directors.json'
LOG_DIR        = 'assets/logs'
LOG_FILE       = os.path.join(LOG_DIR, 'director_fetch.log')
MAX_WORKERS    = 10
MAX_PENDING    = 50
RETRIES        = 3
RETRY_DELAY    = 5

# ─── Ensure log directory exists ────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)

# ─── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)

def load_relevant():
    df = pd.read_csv(RELEVANT_CSV, dtype=str)
    return [n for n in df['Company Number'].dropna().unique()]

def load_existing():
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON) as f:
            return json.load(f)
    return {}

def fetch_one(number):
    for attempt in range(1, RETRIES+1):
        enforce_rate_limit()
        resp = requests.get(f"{API_BASE}/{number}/officers",
                            auth=(CH_KEY,''), timeout=10)
        if resp.status_code >= 500 and attempt < RETRIES:
            log.warning(f"{number}: {resp.status_code}, retrying")
            time.sleep(RETRY_DELAY)
            continue
        try:
            resp.raise_for_status()
            record_call()
            items = resp.json().get('items', [])
        except Exception as e:
            log.warning(f"{number}: fetch error: {e}")
            items = []
        break

    ROLES = {'director','member'}
    active = [o for o in items if o.get('officer_role') in ROLES and o.get('resigned_on') is None]
    chosen = active or [o for o in items if o.get('officer_role') in ROLES]

    directors = []
    for off in chosen:
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
            'appointment':      off.get('snippet',''),
            'dateOfBirth':      dob_str,
            'appointmentCount': off.get('appointment_count'),
            'selfLink':         off['links'].get('self'),
            'officerRole':      off.get('officer_role'),
            'nationality':      off.get('nationality'),
            'occupation':       off.get('occupation'),
        })

    return number, directors

def main():
    log.info("Starting fetch_directors")
    if not CH_KEY:
        log.error("CH_API_KEY unset")
        return

    existing = load_existing()
    log.info(f"Loaded {len(existing)} existing entries")

    relevant = load_relevant()
    pending = [n for n in relevant if n not in existing][:MAX_PENDING]
    log.info(f"{len(pending)} companies pending")

    if pending:
        with ThreadPoolExecutor(MAX_WORKERS) as exe:
            futures = {exe.submit(fetch_one, num): num for num in pending}
            for fut in as_completed(futures):
                num, dirs = fut.result()
                existing[num] = dirs
                log.info(f"Fetched {len(dirs)} for {num}")

    # Write merged JSON
    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
    with open(DIRECTORS_JSON, 'w') as f:
        json.dump(existing, f, separators=(',',':'))
    log.info(f"Wrote directors.json with {len(existing)} entries")

if __name__ == '__main__':
    main()
