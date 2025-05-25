#!/usr/bin/env python3
"""
fetch_directors.py

- Fetches today’s new directors for relevant companies
- Caps to MAX_PENDING per run
- Uses up to MAX_WORKERS threads
- Retries transient 5xx errors up to RETRIES times
- Respects 600 calls/5min via rate_limiter
- Logs to assets/logs/fund_tracker.log
"""

import os
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from rate_limiter import enforce_rate_limit, record_call
from logger import log, LOG_FILE

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
API_BASE        = 'https://api.company-information.service.gov.uk/company'
CH_KEY          = os.getenv('CH_API_KEY')
RELEVANT_CSV    = 'assets/data/relevant_companies.csv'
DIRECTORS_JSON  = 'assets/data/directors.json'
MAX_WORKERS     = 10      # parallel threads
MAX_PENDING     = 50      # at most this many companies per run
RETRIES         = 3       # number of attempts per company
RETRY_DELAY     = 5       # seconds between retries

def load_relevant_numbers():
    df = pd.read_csv(RELEVANT_CSV, dtype=str, usecols=['Company Number'])
    return [num for num in df['Company Number'].dropna().unique()]

def load_existing_map():
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON) as f:
            return json.load(f)
    return {}

def fetch_one(number):
    for attempt in range(1, RETRIES + 1):
        enforce_rate_limit()
        try:
            resp = requests.get(
                f"{API_BASE}/{number}/officers",
                auth=(CH_KEY, ''),
                params={'register_view': 'true'},
                timeout=10
            )
            resp.raise_for_status()
            record_call()
            items = resp.json().get('items', [])
            break
        except requests.HTTPError as e:
            status = e.response.status_code if e.response else '??'
            if 500 <= status < 600 and attempt < RETRIES:
                log.warning(f"{number}: HTTP {status} on attempt {attempt}, retrying in {RETRY_DELAY}s")
                time.sleep(RETRY_DELAY)
                continue
            log.warning(f"{number}: fetch error: {e} (status={status}, attempt={attempt})")
            return number, None
        except Exception as e:
            log.warning(f"{number}: fetch error: {e} (attempt {attempt})")
            return number, None

    # filter roles
    ROLES = {'director', 'member'}
    active = [o for o in items if o.get('officer_role') in ROLES and o.get('resigned_on') is None]
    chosen = active or [o for o in items if o.get('officer_role') in ROLES]

    directors = []
    for off in chosen:
        dob = off.get('date_of_birth') or {}
        y, m = dob.get('year'), dob.get('month')
        dob_str = f"{y}-{int(m):02d}" if y and m else str(y) if y else ''
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
    log.info("Starting director fetch run")

    if not CH_KEY:
        log.error("CH_API_KEY missing")
        return

    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)

    relevant = load_relevant_numbers()
    log.info(f"Total relevant companies: {len(relevant)}")

    existing = load_existing_map()
    pending_all = [num for num in relevant if num not in existing]
    pending = pending_all[:MAX_PENDING]
    log.info(f"Pending companies to fetch (capped at {MAX_PENDING}): {len(pending)}")

    if not pending:
        log.info("No new companies to fetch directors for.")
    else:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_one, num): num for num in pending}
            for future in as_completed(futures):
                num = futures[future]
                try:
                    num_ret, dirs = future.result()
                except Exception as e:
                    log.warning(f"{num}: unexpected error: {e}")
                    continue
                if dirs:
                    existing[num_ret] = dirs
                    log.info(f"Fetched {len(dirs)} director(s) for {num_ret}")

    # Always write directors.json
    with open(DIRECTORS_JSON, 'w') as f:
        json.dump(existing, f, separators=(',',':'))
    log.info(f"Wrote directors.json with {len(existing)} companies")

if __name__ == '__main__':
    main()
