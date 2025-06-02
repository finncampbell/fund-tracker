#!/usr/bin/env python3
"""
fetch_directors.py

- Reads docs/assets/data/relevant_companies.csv and loads docs/assets/data/directors.json.
- Fetches officers for any companyNumber not already in directors.json.
- Respects the global 600 calls/5-minute window (via rate_limiter.py).
- Captures all active officer roles (natural, corporate, nominee, LLP-members, etc.).
- Logs to assets/logs/director_fetch.log.
"""

import os
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from rate_limiter import enforce_rate_limit, record_call

# ─── Configuration ───────────────────────────────────────────────────────────────
API_BASE       = 'https://api.company-information.service.gov.uk/company'
CH_KEY         = os.getenv('CH_API_KEY')
RELEVANT_CSV   = 'docs/assets/data/relevant_companies.csv'
DIRECTORS_JSON = 'docs/assets/data/directors.json'
LOG_DIR        = 'assets/logs'
LOG_FILE       = os.path.join(LOG_DIR, 'director_fetch.log')
MAX_WORKERS    = 100

# Officer roles to treat as “director” (only active ones, i.e. resigned_on is None)
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

# ─── Logging Setup ───────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)
log = logging.getLogger(__name__)


def load_relevant_company_numbers():
    """
    Read relevant_companies.csv and return a list of all CompanyNumber strings.
    """
    if not os.path.exists(RELEVANT_CSV):
        log.error(f"{RELEVANT_CSV} not found")
        return []
    df = pd.read_csv(RELEVANT_CSV, dtype=str)
    if 'CompanyNumber' not in df.columns:
        log.error(f"Expected 'CompanyNumber' column in {RELEVANT_CSV}; found: {df.columns.tolist()}")
        return []
    return df['CompanyNumber'].dropna().tolist()


def load_existing_directors():
    """
    Load directors.json (if it exists) or return an empty dict.
    The JSON maps CompanyNumber -> [list of director dicts].
    """
    if os.path.exists(DIRECTORS_JSON):
        try:
            return json.load(open(DIRECTORS_JSON, 'r'))
        except json.JSONDecodeError:
            log.warning(f"Corrupt {DIRECTORS_JSON}; starting with empty dictionary")
            return {}
    return {}


def save_directors(directors_map):
    """
    Atomically write directors_map (a dict) to DIRECTORS_JSON.
    """
    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
    tmp = DIRECTORS_JSON + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(directors_map, f, separators=(',', ':'))
    os.replace(tmp, DIRECTORS_JSON)


def fetch_officers_for_company(number):
    """
    Hit /company/{number}/officers, retry up to 3 times on 5xx or network errors.
    Always blocks on enforce_rate_limit() before attempting each request, and calls record_call()
    immediately after each HTTP interaction, even on exception. Then filters to active ROLES.
    Returns (company_number, [list of director dicts]).
    """
    RETRIES, DELAY = 3, 5
    items = []

    for attempt in range(RETRIES):
        # Block until we're under 600 calls in the last 5 minutes
        enforce_rate_limit()

        try:
            resp = requests.get(
                f"{API_BASE}/{number}/officers",
                auth=(CH_KEY, ''),
                timeout=10
            )
        except Exception as e:
            # Network error: count it, log, then retry after delay
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

    # Filter to all active officer roles in ROLES
    chosen = [
        o for o in items
        if o.get('officer_role') in ROLES and o.get('resigned_on') is None
    ]

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
            'title':            o.get('name'),
            'appointment':      o.get('snippet', ''),
            'dateOfBirth':      dob_str,
            'appointmentCount': o.get('appointment_count'),
            'selfLink':         o.get('links', {}).get('self'),
            'officerRole':      o.get('officer_role'),
            'nationality':      o.get('nationality'),
            'occupation':       o.get('occupation'),
        })

    return number, directors_list


def main():
    log.info("Starting fetch_directors.py")

    # 1) Load the list of all “relevant” company numbers
    relevant_numbers = load_relevant_company_numbers()
    if not relevant_numbers:
        log.info("No relevant companies found. Exiting.")
        return

    # 2) Load existing directors (if any)
    existing = load_existing_directors()

    # 3) Build a list of “pending” companies (in relevant CSV but not in directors.json)
    pending = [num for num in relevant_numbers if num not in existing]
    total = len(pending)
    log.info(f"{total} companies pending director-fetch")

    if total == 0:
        log.info("All relevant companies already have directors in JSON. Exiting.")
        return

    idx = 0
    while idx < total:
        batch_size = min(MAX_WORKERS, total - idx)
        batch = pending[idx : idx + batch_size]
        log.info(f"Dispatching batch {idx + 1}-{idx + batch_size} of {total}")

        with ThreadPoolExecutor(max_workers=batch_size) as exe:
            future_to_num = {exe.submit(fetch_officers_for_company, num): num for num in batch}
            for fut in as_completed(future_to_num):
                comp = future_to_num[fut]
                try:
                    num, dirs = fut.result()
                except Exception as e:
                    log.error(f"Error fetching officers for {comp}: {e}")
                    continue

                existing[num] = dirs
                log.info(f"Fetched {len(dirs)} active directors for {num}")

        idx += batch_size

    # 4) Write out the updated directors.json
    save_directors(existing)
    log.info(f"Completed fetch; total director entries now: {len(existing)}")


if __name__ == '__main__':
    main()
