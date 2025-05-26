#!/usr/bin/env python3
"""
fetch_directors.py

- Fetches new companies’ directors in dynamic batches
- Honors a buffered rate limit (550 calls per 5 min)
- Sleeps and retries when quota is exhausted
- Writes directly to docs/assets/data/directors.json
- Logs to assets/logs/director_fetch.log
"""

import os
import json
import time
import argparse
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
LOG_FILE       = os.path.join(LOG_DIR, 'director_fetch.log')
RETRIES        = 3
RETRY_DELAY    = 5
WINDOW_SECONDS = 300  # 5 minutes

# ─── Ensure log directory exists ────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)

# ─── Logger ─────────────────────────────────────────────────────────────────────
log = get_logger('director_fetch', LOG_FILE)


def load_relevant():
    df = pd.read_csv(RELEVANT_CSV, dtype=str)
    return [n for n in df['Company Number'].dropna().unique()]


def load_existing():
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON) as f:
            return json.load(f)
    return {}


def fetch_one(number):
    """Fetch officers for one company, with retries and rate-limiter."""
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

    ROLES = {'director', 'member'}
    active = [o for o in items if o.get('officer_role') in ROLES and o.get('resigned_on') is None]
    chosen = active or [o for o in items if o.get('officer_role') in ROLES]

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


def dynamic_batch_fetch(pending, existing):
    """
    Process pending company numbers in batches sized to the remaining buffered quota.
    Sleeps WINDOW_SECONDS whenever quota is exhausted, and loops until done.
    """
    total = len(pending)
    i = 0

    while i < total:
        available = get_remaining_calls()
        if available == 0:
            log.info(f"No remaining calls; sleeping for {WINDOW_SECONDS}s")
            time.sleep(WINDOW_SECONDS)
            continue

        batch = pending[i : i + available]
        log.info(f"Processing batch {i // available + 1}: {len(batch)} of {total} pending")

        with ThreadPoolExecutor(max_workers=len(batch)) as exe:
            futures = {exe.submit(fetch_one, num): num for num in batch}
            for fut in as_completed(futures):
                num, dirs = fut.result()
                existing[num] = dirs
                log.info(f"Fetched {len(dirs)} officers for {num}")

        i += len(batch)
        # old timestamps will age out automatically, so next get_remaining_calls() sees true available


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', help='not used by fetch_directors', default='')
    parser.add_argument('--end_date',   help='not used by fetch_directors', default='')
    args = parser.parse_args()

    log.info("Starting fetch_directors")

    if not CH_KEY:
        log.error("CH_API_KEY unset")
        return

    existing = load_existing()
    log.info(f"Loaded {len(existing)} existing entries")

    relevant = load_relevant()
    pending = [n for n in relevant if n not in existing]
    total = len(pending)
    log.info(f"{total} companies pending")

    if pending:
        dynamic_batch_fetch(pending, existing)

    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
    with open(DIRECTORS_JSON, 'w') as f:
        json.dump(existing, f, separators=(',',':'))
    log.info(f"Wrote directors.json with {len(existing)} entries")


if __name__ == '__main__':
    main()
