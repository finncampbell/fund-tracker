#!/usr/bin/env python3
"""
backfill_directors.py

- Historical backfill of directors for a given date range
- Reads from docs/assets/data/relevant_companies.csv
- Writes merged results into docs/assets/data/directors.json
- Logs to assets/logs/backfill_directors.log
"""

import os
import json
import argparse
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from rate_limiter import enforce_rate_limit, record_call
from logger import get_logger

# ─── Config ─────────────────────────────────────────────────────────────────────
API_BASE       = 'https://api.company-information.service.gov.uk/company'
CH_KEY         = os.getenv('CH_API_KEY')
RELEVANT_CSV   = 'docs/assets/data/relevant_companies.csv'
DIRECTORS_JSON = 'docs/assets/data/directors.json'
LOG_FILE       = 'assets/logs/backfill_directors.log'
MAX_WORKERS    = 10
MAX_PENDING    = 50
RETRIES        = 3
RETRY_DELAY    = 5

# Configure logger
log = get_logger('backfill_directors', LOG_FILE)

def load_relevant():
    df = pd.read_csv(
        RELEVANT_CSV,
        dtype=str,
        parse_dates=['Incorporation Date']
    )
    return df

def load_existing():
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON) as f:
            return json.load(f)
    return {}

def fetch_officers(number):
    for attempt in range(1, RETRIES + 1):
        enforce_rate_limit()
        try:
            resp = requests.get(
                f"{API_BASE}/{number}/officers",
                auth=(CH_KEY, ''),
                params={'register_view': 'true'},
                timeout=10
            )
            if resp.status_code >= 500 and attempt < RETRIES:
                log.warning(f"{number}: HTTP {resp.status_code}, retry {attempt}")
                time.sleep(RETRY_DELAY)
                continue
            resp.raise_for_status()
            record_call()
            items = resp.json().get('items', [])
        except Exception as e:
            log.warning(f"{number}: fetch error: {e}")
            items = []
        break

    ROLES = {'director', 'member'}
    active = [o for o in items if o.get('officer_role') in ROLES and o.get('resigned_on') is None]
    chosen = active or [o for o in items if o.get('officer_role') in ROLES]

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
            'appointment':      o.get('snippet',''),
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
    parser.add_argument('--start_date', required=True, help='YYYY-MM-DD')
    parser.add_argument('--end_date',   required=True, help='YYYY-MM-DD')
    args = parser.parse_args()

    log.info(f"Starting backfill_directors {args.start_date} → {args.end_date}")
    if not CH_KEY:
        log.error('CH_API_KEY unset')
        return

    df = load_relevant()
    existing = load_existing()

    sd = datetime.fromisoformat(args.start_date).date()
    ed = datetime.fromisoformat(args.end_date).date()

    mask_pending = ~df['Company Number'].isin(existing.keys())
    mask_window  = df['Incorporation Date'].dt.date.between(sd, ed)
    pending = df[mask_pending & mask_window]['Company Number'].tolist()[:MAX_PENDING]

    log.info(f"Backfill window: {len(pending)} companies pending")

    if pending:
        with ThreadPoolExecutor(MAX_WORKERS) as exe:
            futures = {exe.submit(fetch_officers, num): num for num in pending}
            for fut in as_completed(futures):
                num, dirs = fut.result()
                existing[num] = dirs
                log.info(f"Backfilled {len(dirs)} officers for {num}")

    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
    with open(DIRECTORS_JSON, 'w') as f:
        json.dump(existing, f, separators=(',',':'))
    log.info(f"Wrote directors.json with {len(existing)} entries")

if __name__ == '__main__':
    main()
