#!/usr/bin/env python3
"""
fetch_directors.py

- Fetches directors for every filtered company in relevant_companies.csv
- Skips companies already in no_directors.json
- Records any company that returns [] into no_directors.json (with a timestamp)
- Removes from no_directors.json once directors appear
- If relevant_companies.csv is missing locally, fetches it from the data branch via raw GitHub URL
- Honors shared buffered rate limit (1200 calls per 5 minutes, as before)
- Updates docs/assets/data/directors.json and docs/assets/data/no_directors.json
- Logs to assets/logs/director_fetch.log
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

from rate_limiter import enforce_rate_limit, record_call, get_remaining_calls, WINDOW_SECONDS, _lock

# ─── Config ─────────────────────────────────────────────────────────────────────
API_BASE          = 'https://api.company-information.service.gov.uk/company'
CH_KEY            = os.getenv('CH_API_KEY')

# Local paths
RELEVANT_CSV      = 'docs/assets/data/relevant_companies.csv'
DIRECTORS_JSON    = 'docs/assets/data/directors.json'
NO_DIRECTORS_JSON = 'docs/assets/data/no_directors.json'

# If local CSV/JSON are missing, fetch from this raw‐GitHub URL (data branch)
GITHUB_USER       = '<YOUR_GITHUB_USER>'
REPO_NAME         = '<YOUR_REPO>'
DATA_BRANCH       = 'data'
RAW_BASE          = f'https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/{DATA_BRANCH}/docs/assets/data'
REMOTE_RELEVANT_CSV   = f'{RAW_BASE}/relevant_companies.csv'
REMOTE_DIRECTORS_JSON = f'{RAW_BASE}/directors.json'

LOG_DIR           = 'assets/logs'
LOG_FILE          = os.path.join(LOG_DIR, 'director_fetch.log')

# Maximum threads per batch
MAX_WORKERS       = 100

# ─── Logging Setup ───────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)

# ─── Helpers: load & save JSON files ─────────────────────────────────────────────
def load_json(path: str) -> dict:
    """
    Safely load a JSON file as a dict. If missing or corrupt, return {}.
    """
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        log.warning(f"Could not read or parse {path}; starting fresh.")
        return {}

def save_json(path: str, data: dict) -> None:
    """
    Atomically save a dict to JSON (via a .tmp → replace).
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(data, f, separators=(',', ':'))
    os.replace(tmp, path)

# ─── Load relevant company numbers, with remote fallback ─────────────────────────
def load_relevant() -> list[str]:
    """
    Load CompanyNumber list from local relevant_companies.csv; if missing or empty,
    attempt to download from the data branch raw URL.
    """
    # Try local file first
    try:
        df = pd.read_csv(RELEVANT_CSV, dtype=str)
        if 'CompanyNumber' not in df.columns:
            log.error(f"Expected 'CompanyNumber' column in {RELEVANT_CSV}; found {df.columns.tolist()}")
            return []
        return df['CompanyNumber'].dropna().astype(str).tolist()
    except FileNotFoundError:
        log.warning(f"Local {RELEVANT_CSV} not found; attempting remote fetch from {REMOTE_RELEVANT_CSV}")
    except pd.errors.EmptyDataError:
        log.warning(f"Local {RELEVANT_CSV} is empty; falling back to remote.")

    # Fallback: download CSV from remote URL
    try:
        log.info(f"Fetching remote relevant_companies.csv from {REMOTE_RELEVANT_CSV}")
        resp = requests.get(REMOTE_RELEVANT_CSV, timeout=15)
        resp.raise_for_status()
        # Load into pandas from the in-memory text
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text), dtype=str)
        if 'CompanyNumber' not in df.columns:
            log.error(f"Expected 'CompanyNumber' in remote CSV from {REMOTE_RELEVANT_CSV}")
            return []
        return df['CompanyNumber'].dropna().astype(str).tolist()
    except Exception as e:
        log.error(f"Failed to load relevant_companies.csv from remote: {e}")
        return []

# ─── Fetch one company’s officers ────────────────────────────────────────────────
def fetch_one(number: str) -> tuple[str, list[dict]]:
    """
    Fetch "officers" endpoint for a single company number.
    Up to 3 retries on server/connection errors.
    Always calls enforce_rate_limit() before each request, then record_call() after.
    Returns (companyNumber, directors_list).
    """
    RETRIES, DELAY = 3, 5  # seconds
    items = []
    for attempt in range(RETRIES):
        enforce_rate_limit()
        try:
            resp = requests.get(
                f"{API_BASE}/{number}/officers",
                auth=(CH_KEY, ''),
                timeout=10
            )
        except Exception as e:
            # Count the call, then retry
            record_call()
            log.warning(f"Network error fetching {number}: {e}; retry {attempt+1}")
            if attempt < RETRIES - 1:
                time.sleep(DELAY)
                continue
            else:
                break

        # Count HTTP interaction
        record_call()

        if resp.status_code >= 500 and attempt < RETRIES - 1:
            log.warning(f"Server error {resp.status_code} for {number}, retry {attempt+1}")
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

# ─── Main logic ─────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', help='unused', default='')
    parser.add_argument('--end_date',   help='unused', default='')
    args = parser.parse_args()

    log.info("Starting fetch_directors")

    # 1) Load existing directors.json and no_directors.json
    existing_dirs: dict[str, list] = load_json(DIRECTORS_JSON)
    no_directors: dict[str, str] = load_json(NO_DIRECTORS_JSON)
    # no_directors example: { "12345678": "2025-02-15", … }

    # 2) Load relevant companies (with remote fallback)
    relevant = load_relevant()

    if not relevant:
        log.error("No companies to fetch (relevant list is empty). Exiting.")
        return

    # 3) Build pending list:
    #    - Include any company NOT in existing_dirs
    #    - Exclude any company already in no_directors (we’ll retry those separately)
    pending = [n for n in relevant if n not in existing_dirs and n not in no_directors]
    total   = len(pending)
    idx     = 0

    log.info(f"{len(existing_dirs)} existing entries; skipping {len(no_directors)} known-no-director companies")
    log.info(f"Pending fetch batch = {total} companies")

    # 4) Batch‐fetch pending at up to MAX_WORKERS or available calls
    while idx < total:
        avail = get_remaining_calls()
        if avail <= 0:
            # Rate limit reached; sleep briefly
            with _lock:
                wait = 0.1
            time.sleep(max(wait, 0.1))
            continue

        batch_size = min(avail, MAX_WORKERS, total - idx)
        batch = pending[idx : idx + batch_size]
        log.info(f"Dispatching batch {idx+1}–{idx+batch_size} of {total}")
        with ThreadPoolExecutor(max_workers=batch_size) as exe:
            future_to_num = {exe.submit(fetch_one, num): num for num in batch}
            for fut in as_completed(future_to_num):
                num, dirs = fut.result()
                if dirs:
                    # Found at least one director → record under directors.json
                    existing_dirs[num] = dirs
                    # If it existed in no_directors.json, remove it
                    if num in no_directors:
                        no_directors.pop(num, None)
                    log.info(f"[{num}] → fetched {len(dirs)} director(s); saved.")
                else:
                    # Returned empty → record (with “first seen” date if new)
                    if num not in no_directors:
                        first_seen = datetime.utcnow().date().isoformat()
                        no_directors[num] = first_seen
                        log.info(f"[{num}] → no directors found; adding to no_directors.json (first seen {first_seen}).")
                    else:
                        log.info(f"[{num}] → no directors found (already in no_directors).")

        idx += batch_size

    # 5) Write out updated JSONs (locally)
    save_json(DIRECTORS_JSON, existing_dirs)
    save_json(NO_DIRECTORS_JSON, no_directors)

    log.info(f"Fetch cycle complete: directors.json has {len(existing_dirs)} entries; no_directors.json has {len(no_directors)} entries.")

if __name__ == '__main__':
    main()
