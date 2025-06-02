#!/usr/bin/env python3
"""
fetch_directors.py

- Fetches directors for every filtered company in relevant_companies.csv
- Dispatches in dynamic batches sized up to MAX_WORKERS
- Honors shared rate limit (600 calls per 5 minutes) via rate_limiter.enforce_rate_limit()
- Updates docs/assets/data/directors.json
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
# (Removed internal _call_times, _lock imports and get_remaining_calls)

# Config
API_BASE       = 'https://api.company-information.service.gov.uk/company'
CH_KEY         = os.getenv('CH_API_KEY')
RELEVANT_CSV   = 'docs/assets/data/relevant_companies.csv'
DIRECTORS_JSON = 'docs/assets/data/directors.json'
LOG_DIR        = 'assets/logs'
LOG_FILE       = os.path.join(LOG_DIR, 'director_fetch.log')

# Pick a sensible upper bound for concurrent threads
MAX_WORKERS    = 100

# Logging
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

def load_relevant():
    df = pd.read_csv(RELEVANT_CSV, dtype=str)
    if 'CompanyNumber' not in df.columns:
        log.error(f"Expected 'CompanyNumber' column in {RELEVANT_CSV}; found {df.columns.tolist()}")
        return []
    return df['CompanyNumber'].dropna().tolist()

def load_existing():
    if os.path.exists(DIRECTORS_JSON):
        try:
            return json.load(open(DIRECTORS_JSON, 'r'))
        except json.JSONDecodeError:
            log.warning("Corrupt directors.json; starting with empty dictionary")
            return {}
    return {}

def fetch_one(number):
    """
    Fetch "officers" endpoint for a single company number.
    Implements up to 3 retries on 5xx, but each attempt blocks on enforce_rate_limit().
    """
    RETRIES, DELAY = 3, 5
    items = []

    for attempt in range(RETRIES):
        # Block here if we've flooded 600 calls in the last 5 minutes
        enforce_rate_limit()

        try:
            resp = requests.get(
                f"{API_BASE}/{number}/officers",
                auth=(CH_KEY, ''),
                timeout=10
            )
        except Exception as e:
            # network failure; count the call anyway, then retry
            record_call()
            log.warning(f"Network error fetching {number}: {e}")
            if attempt < RETRIES - 1:
                time.sleep(DELAY)
                continue
            else:
                break

        # Count every HTTP interaction
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

    ROLES = {'director', 'member'}
    active = [o for o in items if o.get('officer_role') in ROLES and o.get('resigned_on') is None]
    chosen = active or [o for o in items if o.get('officer_role') in ROLES]

    directors_list = []
    for o in chosen:
        dob = o.get('date_of_birth', {}) or {}
        if dob.get('year') and dob.get('month'):
            dob_str = f"{dob['year']}-{int(dob['month']):02d}"
        elif dob.get('year'):
            dob_str = str(dob['year'])
        else:
            dob_str = ""

        directors_list.append({
            'title':           o.get('name'),
            'appointment':     o.get('snippet', ''),
            'dateOfBirth':     dob_str,
            'appointmentCount':o.get('appointment_count'),
            'selfLink':        o.get('links', {}).get('self'),
            'officerRole':     o.get('officer_role'),
            'nationality':     o.get('nationality'),
            'occupation':      o.get('occupation'),
        })

    return number, directors_list

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', help='unused', default='')
    parser.add_argument('--end_date',   help='unused', default='')
    args = parser.parse_args()

    log.info("Starting fetch_directors")
    existing = load_existing()
    relevant = load_relevant()
    pending  = [n for n in relevant if n not in existing]
    total    = len(pending)
    idx = 0

    while idx < total:
        # Always launch up to MAX_WORKERS threads—each thread will block inside fetch_one() if needed
        batch_size = min(MAX_WORKERS, total - idx)
        batch = pending[idx : idx + batch_size]
        log.info(f"Dispatching batch {idx + 1}-{idx + batch_size} of {total}")

        with ThreadPoolExecutor(max_workers=batch_size) as exe:
            future_to_num = {exe.submit(fetch_one, num): num for num in batch}
            for fut in as_completed(future_to_num):
                try:
                    num, dirs = fut.result()
                except Exception as e:
                    log.error(f"Error fetching {future_to_num[fut]}: {e}")
                    continue

                existing[num] = dirs
                log.info(f"Fetched {len(dirs)} officers for {num}")

        idx += batch_size

    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
    temp = DIRECTORS_JSON + ".tmp"
    with open(temp, 'w') as f:
        json.dump(existing, f, separators=(',', ':'))
    os.replace(temp, DIRECTORS_JSON)
    log.info(f"Wrote directors.json with {len(existing)} entries")

if __name__ == '__main__':
    main()
