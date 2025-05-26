#!/usr/bin/env python3
"""
backfill_directors.py

- For a given date range, finds all “relevant” companies created in that window
  (based on your master list), and fetches officers for any that aren’t yet
  in docs/assets/data/directors.json.
- Dispatches in dynamic batches sized to your remaining API quota
- Honors the shared buffered rate limit (600 calls − 50 buffer)
- Logs progress and writes merged directors.json at the end
"""

import os
import json
import time
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from rate_limiter import enforce_rate_limit, record_call, get_remaining_calls

# ─── Configuration ─────────────────────────────────────────────────────────────
API_BASE        = 'https://api.company-information.service.gov.uk/company'
CH_KEY          = os.getenv('CH_API_KEY')
MASTER_CSV      = 'docs/assets/data/master_companies.csv'
DIRECTORS_JSON  = 'docs/assets/data/directors.json'
LOG_DIR         = 'assets/logs'
LOG_FILE        = os.path.join(LOG_DIR, 'backfill_directors.log')
DATE_COL        = 'Date of creation'   # adjust if your master CSV uses a different header

# ─── Logging Setup ─────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)

def load_master(start_date, end_date):
    """Load master list, filter by creation date, return company numbers."""
    df = pd.read_csv(MASTER_CSV, dtype=str, parse_dates=[DATE_COL])
    mask = (df[DATE_COL] >= pd.to_datetime(start_date)) & (df[DATE_COL] <= pd.to_datetime(end_date))
    return df.loc[mask, 'Company Number'].dropna().unique().tolist()

def load_existing():
    """Load any directors we’ve already fetched."""
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def fetch_one(number):
    """Fetch officers for one company, with retries and rate-limiting."""
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

    # Filter and normalize as in fetch_directors.py
    ROLES   = {'director', 'member'}
    active  = [o for o in items if o.get('officer_role') in ROLES and o.get('resigned_on') is None]
    chosen  = active or [o for o in items if o.get('officer_role') in ROLES]

    directors = []
    for o in chosen:
        dob = o.get('date_of_birth') or {}
        y, m = dob.get('year'), dob.get('month')
        dob_str = f"{y}-{int(m):02d}" if y and m else (str(y) if y else '')
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
    parser.add_argument('--start_date', required=True, help='YYYY-MM-DD')
    parser.add_argument('--end_date',   required=True, help='YYYY-MM-DD')
    args = parser.parse_args()

    if not CH_KEY:
        log.error("CH_API_KEY unset")
        return

    # 1) Determine which companies need backfill
    all_to_backfill = load_master(args.start_date, args.end_date)
    existing = load_existing()
    pending = [num for num in all_to_backfill if num not in existing]
    total = len(pending)
    log.info(f"{total} companies to backfill directors for ({args.start_date} to {args.end_date})")

    idx = 0
    processed = 0

    # 2) Dynamic, quota-aware batching
    while idx < total:
        available = get_remaining_calls()
        if available <= 0:
            time.sleep(1)
            continue

        batch = pending[idx: idx + available]
        log.info(f"Dispatching backfill batch {idx+1}–{idx+len(batch)} of {total}")

        with ThreadPoolExecutor(max_workers=len(batch)) as executor:
            futures = {executor.submit(fetch_one, num): num for num in batch}
            for fut in as_completed(futures):
                num, dirs = fut.result()
                existing[num] = dirs
                processed += 1
                log.info(f"Backfilled {len(dirs)} directors for {num} ({processed}/{total})")

        idx += len(batch)

    # 3) Write merged directors.json
    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
    with open(DIRECTORS_JSON, 'w', encoding='utf-8') as f:
        json.dump(existing, f, separators=(',', ':'))
    log.info(f"Wrote directors.json with {len(existing)} total entries")

if __name__ == '__main__':
    main()
