#!/usr/bin/env python3
"""
backfill_directors.py

- Historical backfill within start–end date
- Emits backfill_status.json for UI progress
- Dynamically batches up to MAX_WORKERS
- Updates docs/assets/data/directors.json
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

from rate_limiter import enforce_rate_limit, record_call
from logger import log as root_log

API_BASE       = 'https://api.company-information.service.gov.uk/company'
CH_KEY         = os.getenv('CH_API_KEY')
RELEVANT_CSV   = 'docs/assets/data/relevant_companies.csv'
DIRECTORS_JSON = 'docs/assets/data/directors.json'
STATUS_FILE    = 'docs/assets/data/backfill_status.json'
LOG_DIR        = 'assets/logs'
LOG_FILE       = os.path.join(LOG_DIR, 'backfill_directors.log')

MAX_WORKERS    = 100

os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = root_log.getChild('backfill_directors')

def write_status(total, processed):
    os.makedirs(os.path.dirname(STATUS_FILE), exist_ok=True)
    temp = STATUS_FILE + ".tmp"
    with open(temp, 'w') as f:
        json.dump({'total': total, 'processed': processed}, f)
    os.replace(temp, STATUS_FILE)

def load_relevant():
    df = pd.read_csv(RELEVANT_CSV, dtype=str, parse_dates=['IncorporationDate'])
    return df

def load_existing():
    if os.path.exists(DIRECTORS_JSON):
        try:
            return json.load(open(DIRECTORS_JSON, 'r'))
        except json.JSONDecodeError:
            log.warning("Corrupt directors.json; starting with empty dictionary")
            return {}
    return {}

def fetch_officers(number):
    RETRIES, DELAY = 3, 5
    items = []
    for attempt in range(RETRIES):
        # Block until under 600 calls in 5 min, then record one
        enforce_rate_limit()

        try:
            resp = requests.get(
                f"{API_BASE}/{number}/officers",
                auth=(CH_KEY, ''),
                params={'register_view': 'true'},
                timeout=10
            )
        except Exception as e:
            record_call()
            log.warning(f"Network error fetching {number}: {e}")
            if attempt < RETRIES - 1:
                time.sleep(DELAY)
                continue
            else:
                break

        record_call()

        if resp.status_code >= 500 and attempt < RETRIES - 1:
            log.warning(f"Server error {resp.status_code} for {number}, retry {attempt + 1}")
            time.sleep(DELAY)
            continue

        try:
            resp.raise_for_status()
            items = resp.json().get('items', [])
        except requests.HTTPError as he:
            log.warning(f"Failed to fetch officers for {number}: {he}")
            items = []
        break

    # Include all relevant officer roles, filter out resigned ones
    ROLES = {
        'director',
        'corporate-director',
        'nominee-director',
        'managing-officer',
        'corporate-managing-officer',
        'llp-designated-member',
        'llp-member',
        'corporate-llp-designated-member',
        'corporate-llp-member'
    }
    chosen = [
        o for o in items
        if o.get('officer_role') in ROLES and o.get('resigned_on') is None
    ]

    officers_list = []
    for o in chosen:
        dob = o.get('date_of_birth', {}) or {}
        if dob.get('year') and dob.get('month'):
            dob_str = f"{dob['year']}-{int(dob['month']):02d}"
        elif dob.get('year'):
            dob_str = str(dob['year'])
        else:
            dob_str = ""

        officers_list.append({
            'title':           o.get('name'),
            'appointment':     o.get('snippet', ''),
            'dateOfBirth':     dob_str,
            'appointmentCount':o.get('appointment_count'),
            'selfLink':        o.get('links', {}).get('self'),
            'officerRole':     o.get('officer_role'),
            'nationality':     o.get('nationality'),
            'occupation':      o.get('occupation'),
        })

    return number, officers_list

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', required=True)
    parser.add_argument('--end_date',   required=True)
    args = parser.parse_args()

    df = load_relevant()
    existing = load_existing()

    sd = datetime.fromisoformat(args.start_date).date()
    ed = datetime.fromisoformat(args.end_date).date()

    mask_new = ~df['CompanyNumber'].isin(existing.keys())
    mask_win = df['IncorporationDate'].dt.date.between(sd, ed)
    pending = df[mask_new & mask_win]['CompanyNumber'].tolist()

    total, processed = len(pending), 0
    write_status(total, processed)
    log.info(f"Backfill start: {total} companies")

    idx = 0
    while idx < total:
        batch_size = min(MAX_WORKERS, total - idx)
        batch = pending[idx: idx + batch_size]
        log.info(f"Dispatching batch {idx + 1}-{idx + batch_size} of {total}")

        with ThreadPoolExecutor(max_workers=batch_size) as exe:
            future_to_num = {exe.submit(fetch_officers, num): num for num in batch}
            for fut in as_completed(future_to_num):
                try:
                    num, officers = fut.result()
                except Exception as e:
                    log.error(f"Error fetching {future_to_num[fut]}: {e}")
                    continue

                existing[num] = officers
                processed += 1
                write_status(total, processed)
                log.info(f"Fetched {len(officers)} active directors for {num} ({processed}/{total})")

        idx += batch_size

    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
    temp = DIRECTORS_JSON + ".tmp"
    with open(temp, 'w') as f:
        json.dump(existing, f, separators=(',', ':'))
    os.replace(temp, DIRECTORS_JSON)
    log.info(f"Wrote directors.json with {len(existing)} entries")

if __name__ == '__main__':
    main()
