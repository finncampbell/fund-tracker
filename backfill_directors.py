#!/usr/bin/env python3
"""
backfill_directors.py

- Historical backfill of directors for a date range
- Uses a pool of up to 550 threads (buffered rate limit)
- Each thread enforces rate-limit & prunes old timestamps automatically
- Emits backfill_status.json for UI progress
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

from rate_limiter import enforce_rate_limit, record_call
from logger import log as base_log  # your existing logger.py exports `log`
# If you prefer basicConfig here:
# logging.basicConfig(...)
# log = logging.getLogger(__name__)

# ─── Configuration ─────────────────────────────────────────────────────────────
API_BASE       = 'https://api.company-information.service.gov.uk/company'
CH_KEY         = os.getenv('CH_API_KEY')
RELEVANT_CSV   = 'docs/assets/data/relevant_companies.csv'
DIRECTORS_JSON = 'docs/assets/data/directors.json'
STATUS_FILE    = 'docs/assets/data/backfill_status.json'
LOG_DIR        = 'assets/logs'
LOG_FILE       = os.path.join(LOG_DIR, 'backfill_directors.log')
RETRIES        = 3
RETRY_DELAY    = 5
MAX_WORKERS    = 550   # buffered cap

# ─── Logging Setup ─────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)


def write_status(total, processed, start_ts, end_ts=None):
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
    df = pd.read_csv(RELEVANT_CSV, dtype=str, parse_dates=['Incorporation Date'])
    return df


def load_existing():
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON) as f:
            return json.load(f)
    return {}


def fetch_officers(number):
    """Fetch officer data for a single company, with retries and rate‐limiting."""
    for attempt in range(1, RETRIES + 1):
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

    ROLES  = {'director','member'}
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
        log.error("CH_API_KEY unset")
        return

    df       = load_relevant()
    existing = load_existing()

    sd = datetime.fromisoformat(args.start_date).date()
    ed = datetime.fromisoformat(args.end_date).date()

    mask_not_done  = ~df['Company Number'].isin(existing.keys())
    mask_in_window = df['Incorporation Date'].dt.date.between(sd, ed)
    pending        = df[mask_not_done & mask_in_window]['Company Number'].tolist()

    total     = len(pending)
    processed = 0
    start_ts  = int(time.time())
    write_status(total, processed, start_ts)
    log.info(f"{total} companies pending for backfill")

    if pending:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futures = {exe.submit(fetch_officers, num): num for num in pending}
            for fut in as_completed(futures):
                num, dirs = fut.result()
                existing[num] = dirs
                processed += 1
                write_status(total, processed, start_ts)
                log.info(f"Fetched {len(dirs)} officers for {num}")

    write_status(total, processed, start_ts, end_ts=int(time.time()))

    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
    with open(DIRECTORS_JSON, 'w') as f:
        json.dump(existing, f, separators=(',',':'))
    log.info(f"Wrote directors.json with {len(existing)} entries")


if __name__ == '__main__':
    main()
