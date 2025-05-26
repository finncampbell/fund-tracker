#!/usr/bin/env python3
"""
backfill_directors.py

- Historical backfill of directors for a given date range
- Emits a backfill_status.json for UI progress
- Dynamically batches work against a buffered rate limit
- Sleeps & retries as needed until all pending companies are processed
- Writes merged results into docs/assets/data/directors.json
- Logs to assets/logs/backfill_directors.log
"""

import os
import json
import time
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from rate_limiter import enforce_rate_limit, record_call, get_remaining_calls
from logger import get_logger

# ─── Config ─────────────────────────────────────────────────────────────────────
API_BASE        = 'https://api.company-information.service.gov.uk/company'
CH_KEY          = os.getenv('CH_API_KEY')
RELEVANT_CSV    = 'docs/assets/data/relevant_companies.csv'
DIRECTORS_JSON  = 'docs/assets/data/directors.json'
STATUS_FILE     = 'docs/assets/data/backfill_status.json'
LOG_DIR         = 'assets/logs'
LOG_FILE        = os.path.join(LOG_DIR, 'backfill_directors.log')
RETRIES         = 3
RETRY_DELAY     = 5
WINDOW_SECONDS  = 300  # 5 minutes

# ─── Ensure log directory exists ────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)

# ─── Logger ─────────────────────────────────────────────────────────────────────
log = get_logger('backfill_directors', LOG_FILE)


def write_status(total, processed, start_ts, end_ts=None):
    """Write backfill progress to a JSON file for the UI to poll."""
    os.makedirs(os.path.dirname(STATUS_FILE), exist_ok=True)
    status = {
        "total":      total,
        "processed":  processed,
        "start_time": start_ts
    }
    if end_ts is not None:
        status["end_time"] = end_ts
    with open(STATUS_FILE, 'w') as f:
        json.dump(status, f)


def load_relevant():
    """Load relevant companies with parsed incorporation dates."""
    df = pd.read_csv(RELEVANT_CSV, dtype=str, parse_dates=['Incorporation Date'])
    return df


def load_existing():
    """Load existing directors.json or return empty dict."""
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON) as f:
            return json.load(f)
    return {}


def fetch_officers(number):
    """Fetch officer data for a single company, with retries and rate-limiting."""
    for attempt in range(1, RETRIES + 1):
        enforce_rate_limit()
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
        try:
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


def dynamic_backfill(pending, existing):
    """
    Process pending company numbers in batches sized to the remaining buffered quota.
    Sleeps and retries until all pending companies are processed.
    """
    total     = len(pending)
    processed = 0
    start_ts  = int(time.time())
    write_status(total, processed, start_ts)

    index = 0
    while index < total:
        available = get_remaining_calls()
        if available == 0:
            log.info(f"No remaining calls; sleeping for {WINDOW_SECONDS}s to reset window")
            time.sleep(WINDOW_SECONDS)
            continue

        batch = pending[index : index + available]
        log.info(f"Backfill batch {index}//{total}: processing {len(batch)} companies")

        with ThreadPoolExecutor(max_workers=len(batch)) as exe:
            futures = {exe.submit(fetch_officers, num): num for num in batch}
            for fut in as_completed(futures):
                num, dirs = fut.result()
                existing[num] = dirs
                processed += 1
                log.info(f"Fetched {len(dirs)} officers for {num}")
                write_status(total, processed, start_ts)

        index += len(batch)
        # old timestamps age out, replenishing quota

    # final write to signal completion
    write_status(total, processed, start_ts, end_ts=int(time.time()))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', required=True, help='YYYY-MM-DD')
    parser.add_argument('--end_date',   required=True, help='YYYY-MM-DD')
    args = parser.parse_args()

    log.info(f"Starting backfill_directors {args.start_date} → {args.end_date}")
    if not CH_KEY:
        log.error("CH_API_KEY unset")
        return

    df       = load_relevant()
    existing = load_existing()

    sd = datetime.fromisoformat(args.start_date).date()
    ed = datetime.fromisoformat(args.end_date).date()

    mask_not_done  = ~df['Company Number'].isin(existing.keys())
    mask_in_window = df['Incorporation Date'].dt.date.between(sd, ed)
    pending        = df[mask_not_done & mask_in_window]['Company Number'].tolist()

    log.info(f"{len(pending)} companies pending in window {args.start_date} → {args.end_date}")

    if pending:
        dynamic_backfill(pending, existing)

    # Write merged JSON of all directors
    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
    with open(DIRECTORS_JSON, 'w') as f:
        json.dump(existing, f, separators=(',',':'))
    log.info(f"Wrote directors.json with {len(existing)} company entries")


if __name__ == '__main__':
    main()
