#!/usr/bin/env python3
"""
fetch_directors.py

- Fetches new companies’ directors in dynamic concurrency
- Processes up to 100 new companies per run
- Uses a pool of up to 550 threads (buffered rate limit)
- Each thread enforces the shared rate limit and prunes old timestamps
- Writes merged results into docs/assets/data/directors.json
- Logs to assets/logs/director_fetch.log
"""

import os
import json
import time
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from rate_limiter import enforce_rate_limit, record_call

# ─── Configuration ─────────────────────────────────────────────────────────────
API_BASE       = 'https://api.company-information.service.gov.uk/company'
CH_KEY         = os.getenv('CH_API_KEY')
RELEVANT_CSV   = 'docs/assets/data/relevant_companies.csv'
DIRECTORS_JSON = 'docs/assets/data/directors.json'
LOG_DIR        = 'assets/logs'
LOG_FILE       = os.path.join(LOG_DIR, 'director_fetch.log')

# How many new companies to process each run
MAX_PENDING    = 100
# Buffered cap: 600 calls − 50 buffer = 550 concurrent workers
MAX_WORKERS    = 550

# ─── Logging Setup ─────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)


def load_relevant():
    df = pd.read_csv(RELEVANT_CSV, dtype=str)
    return df['Company Number'].dropna().unique().tolist()


def load_existing():
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON) as f:
            return json.load(f)
    return {}


def fetch_one(number):
    """Fetch officers for one company, with retries and rate‐limiting."""
    RETRIES     = 3
    RETRY_DELAY = 5

    for attempt in range(1, RETRIES + 1):
        enforce_rate_limit()
        resp = requests.get(
            f"{API_BASE}/{number}/officers",
            auth=(CH_KEY, ''), timeout=10
        )
        if resp.status_code >= 500 and attempt < RETRIES:
            log.warning(f"{number}: HTTP {resp.status_code}, retry {attempt}")
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

    ROLES   = {'director', 'member'}
    active  = [o for o in items if o.get('officer_role') in ROLES and o.get('resigned_on') is None]
    chosen  = active or [o for o in items if o.get('officer_role') in ROLES]

    directors = []
    for o in chosen:
        dob = o.get('date_of_birth') or {}
        y, m = dob.get('year'), dob.get('month')
        if y and m:
            dob_str = f"{y}-{int(m):02d}"
        elif y:
            dob_str = str(y)
        else:
            dob_str = ''

        directors.append({
            'title':            o.get('name'),
            'appointment':      o.get('snippet', ''),
            'dateOfBirth':      dob_str,
            'appointmentCount': o.get('appointment_count'),
            'selfLink':         o['links'].get('self'),
            'officerRole':      o.get('officer_role'),
            'nationality':      o.get('nationality'),
            'occupation':       o.get('occupation'),
        })

    return number, directors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', help='unused', default='')
    parser.add_argument('--end_date',   help='unused', default='')
    args = parser.parse_args()

    log.info("Starting fetch_directors")
    if not CH_KEY:
        log.error("CH_API_KEY unset")
        return

    existing = load_existing()
    log.info(f"Loaded {len(existing)} existing entries")

    relevant_all = load_relevant()
    pending_all  = [n for n in relevant_all if n not in existing]
    pending      = pending_all[:MAX_PENDING]
    log.info(f"{len(pending)} companies pending (of {len(pending_all)} new total)")

    if pending:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_one, num): num for num in pending}
            for fut in as_completed(futures):
                num, dirs = fut.result()
                existing[num] = dirs
                log.info(f"Fetched {len(dirs)} officers for {num}")

    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
    with open(DIRECTORS_JSON, 'w') as f:
        json.dump(existing, f, separators=(',', ':'))
    log.info(f"Wrote directors.json with {len(existing)} entries")


if __name__ == '__main__':
    main()
