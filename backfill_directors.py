#!/usr/bin/env python3
"""
backfill_directors.py

- Historical backfill of directors for a given date range
- Dynamically batches against a shared buffered rate limit (550 calls/5min)
- Spawns up to 550 parallel fetches at once
- Writes merged results into docs/assets/data/directors.json
- Logs to assets/logs/backfill_directors.log
"""

import os
import json
import time
import argparse
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from rate_limiter import enforce_rate_limit, record_call, get_remaining_calls
from logger import get_logger

# ─── Config ─────────────────────────────────────────────────────────────────────
API_BASE       = 'https://api.company-information.service.gov.uk/company'
CH_KEY         = os.getenv('CH_API_KEY')
RELEVANT_CSV   = 'docs/assets/data/relevant_companies.csv'
DIRECTORS_JSON = 'docs/assets/data/directors.json'
LOG_DIR        = 'assets/logs'
LOG_FILE       = os.path.join(LOG_DIR, 'backfill_directors.log')
RETRIES        = 3
RETRY_DELAY    = 5
# Maximum parallel workers = buffered cap (600 - 50)
MAX_WORKERS    = 550

# ─── Logging Setup ─────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = get_logger('backfill_directors', LOG_FILE)

def load_relevant():
    df = pd.read_csv(RELEVANT_CSV, dtype=str, parse_dates=['Incorporation Date'])
    return df

def load_existing():
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON) as f:
            return json.load(f)
    return {}

def fetch_officers(number):
    """Fetch officer data for a single company, with retries & rate‐limiting."""
    for attempt in range(1, RETRIES+1):
        enforce_rate_limit()
        resp = requests.get(
            f"{API_BASE}/{number}/officers",
            auth=(CH_KEY, ''), params={'register_view':'true'}, timeout=10
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

    # Filter to directors/members
    ROLES = {'director','member'}
    active = [o for o in items if o.get('officer_role') in ROLES and o.get('resigned_on') is None]
    chosen = active or [o for o in items if o.get('officer_role') in ROLES]

    # Normalize
    directors = []
    for o in chosen:
        dob = o.get('date_of_birth') or {}
        y, m = dob.get('year'), dob.get('month')
        dob_str = f"{y}-{int(m):02d}" if y and m else (str(y) if y else '')
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

def dynamic_backfill(pending, existing):
    """
    Process pending list in slices sized to the current remaining quota.
    Sleeps only as long as needed when quota is exhausted.
    """
    total     = len(pending)
    processed = 0
    idx       = 0
    log.info(f"Starting backfill of {total} companies")

    while idx < total:
        # How many calls can we make right now?
        available = get_remaining_calls()
        if available <= 0:
            # block until the oldest timestamp rolls off
            log.info("Quota exhausted—sleeping until slots free")
            time.sleep(1)  # short sleep to loop quickly
            continue

        # batch up to available, but never more than MAX_WORKERS
        batch_size = min(available, MAX_WORKERS, total - idx)
        batch = pending[idx:idx+batch_size]
        log.info(f"Dispatching batch {idx+1}-{idx+batch_size} / {total}")

        with ThreadPoolExecutor(max_workers=batch_size) as exe:
            futures = {exe.submit(fetch_officers, num): num for num in batch}
            for fut in as_completed(futures):
                num, dirs = fut.result()
                existing[num] = dirs
                processed += 1
                log.info(f"Fetched {len(dirs)} for {num} ({processed}/{total})")

        idx += batch_size

    log.info("Backfill complete")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', required=True, help='YYYY-MM-DD')
    parser.add_argument('--end_date',   required=True, help='YYYY-MM-DD')
    args = parser.parse_args()

    if not CH_KEY:
        log.error("CH_API_KEY unset")
        return

    df       = load_relevant()
    existing = load_existing()

    sd = datetime.fromisoformat(args.start_date).date()
    ed = datetime.fromisoformat(args.end_date).date()

    mask_new    = ~df['Company Number'].isin(existing.keys())
    mask_window = df['Incorporation Date'].dt.date.between(sd, ed)
    pending     = df[mask_new & mask_window]['Company Number'].tolist()

    if not pending:
        log.info("No new companies to backfill in that window")
    else:
        dynamic_backfill(pending, existing)

    # Write merged JSON
    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
    with open(DIRECTORS_JSON, 'w') as f:
        json.dump(existing, f, separators=(',',':'))
    log.info(f"Wrote directors.json with {len(existing)} entries")

if __name__ == '__main__':
    main()
