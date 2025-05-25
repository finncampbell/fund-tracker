#!/usr/bin/env python3
"""
backfill_directors.py

- Backfills directors over a historical date range
- Caps to MAX_PENDING per run
- Uses up to MAX_WORKERS threads
- Retries transient 5xx errors up to RETRIES times
- Logs to assets/logs/fund_tracker.log
"""

import os
import json
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import requests

from rate_limiter import enforce_rate_limit, record_call
from logger import log, LOG_FILE

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
API_BASE        = 'https://api.company-information.service.gov.uk/company'
CH_KEY          = os.getenv('CH_API_KEY')
RELEVANT_CSV    = 'assets/data/relevant_companies.csv'
DIRECTORS_JSON  = 'assets/data/directors.json'
MAX_WORKERS     = 10
MAX_PENDING     = 50
RETRIES         = 3
RETRY_DELAY     = 5

def load_relevant():
    return pd.read_csv(RELEVANT_CSV, dtype=str, parse_dates=['Incorporation Date'])

def load_existing():
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON) as f:
            return json.load(f)
    return {}

def fetch_officers(number):
    items = []
    for attempt in range(1, RETRIES + 1):
        enforce_rate_limit()
        resp = requests.get(
            f"{API_BASE}/{number}/officers",
            auth=(CH_KEY, ''),
            timeout=10
        )
        status = resp.status_code
        if 500 <= status < 600 and attempt < RETRIES:
            log.warning(f"{number}: HTTP {status} on attempt {attempt}, retrying in {RETRY_DELAY}s")
            time.sleep(RETRY_DELAY)
            continue

        try:
            resp.raise_for_status()
            record_call()
            items = resp.json().get('items', [])
        except Exception as e:
            log.warning(f"{number}: fetch error: {e} (status={status}, attempt={attempt})")
        break

    ROLES = {'director', 'member'}
    active = [o for o in items if o.get('officer_role') in ROLES and o.get('resigned_on') is None]
    chosen = active or [o for o in items if o.get('officer_role') in ROLES]

    directors = []
    for off in chosen:
        dob = off.get('date_of_birth') or {}
        y, m = dob.get('year'), dob.get('month')
        dob_str = f"{y}-{int(m):02d}" if y and m else (str(y) if y else '')
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
    log.info(f"Logging to {LOG_FILE}")
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', required=True, help='YYYY-MM-DD')
    parser.add_argument('--end_date',   required=True, help='YYYY-MM-DD')
    args = parser.parse_args()

    log.info(f"Starting historical backfill {args.start_date} → {args.end_date}")
    if not CH_KEY:
        log.error("CH_API_KEY missing")
        return

    df = load_relevant()
    existing = load_existing()

    sd = datetime.fromisoformat(args.start_date).date()
    ed = datetime.fromisoformat(args.end_date).date()

    mask_pending = ~df['Company Number'].isin(existing.keys())
    mask_hist    = df['Incorporation Date'].dt.date.between(sd, ed)
    df_pending   = df[mask_pending & mask_hist].sort_values('Incorporation Date', ascending=False)

    pending_all = df_pending['Company Number'].tolist()
    pending = pending_all[:MAX_PENDING]
    log.info(f"Pending companies to backfill (capped at {MAX_PENDING}): {len(pending)}")
    log.info(f"Pending company numbers: {pending}")

    if pending:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_officers, num): num for num in pending}
            for future in as_completed(futures):
                num = futures[future]
                try:
                    num_ret, dirs = future.result()
                    existing[num_ret] = dirs
                    log.info(f"Backfilled {len(dirs)} officers for {num_ret}")
                except Exception:
                    log.exception(f"{num}: unexpected exception in fetch_officers")
    else:
        log.info("No historical companies to backfill.")

    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
    with open(DIRECTORS_JSON, 'w') as f:
        json.dump(existing, f, separators=(',',':'))
    log.info(f"Wrote directors.json with {len(existing)} companies")

if __name__ == '__main__':
    main()
