#!/usr/bin/env python3
"""
retry_no_directors.py

- Reads docs/assets/data/no_directors.json
- For each CompanyNumber there, attempts to fetch officers (via the same fetch_one logic)
- If found directors, moves the entry into directors.json and removes from no_directors.json
- If still empty AND first-seen date is > 30 days ago, removes from no_directors.json
- Writes updated directors.json and no_directors.json
- Logs progress to assets/logs/retry_no_directors.log
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from rate_limiter import enforce_rate_limit, record_call, get_remaining_calls, WINDOW_SECONDS, _lock

# ─── Config ───────────────────────────────────────────────────────────────────────
API_BASE          = 'https://api.company-information.service.gov.uk/company'
CH_KEY            = os.getenv('CH_API_KEY')
DIRECTORS_JSON    = 'docs/assets/data/directors.json'
NO_DIRECTORS_JSON = 'docs/assets/data/no_directors.json'
LOG_DIR           = 'assets/logs'
LOG_FILE          = os.path.join(LOG_DIR, 'retry_no_directors.log')

# Max threads per batch
MAX_WORKERS       = 50

# How many days to keep trying before giving up
GIVE_UP_DAYS      = 30

# ─── Logging Setup ───────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger('RetryNoDirectors')

# ─── Helpers: load & save JSON ─────────────────────────────────────────────────────
def load_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        log.warning(f"Could not read/parse {path}; starting fresh.")
        return {}

def save_json(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(data, f, separators=(',', ':'))
    os.replace(tmp, path)

# ─── Fetch one company’s officers (same logic as in fetch_directors) ──────────────
def fetch_one(number: str) -> tuple[str, list[dict]]:
    """
    Identical to fetch_directors.fetch_one
    """
    RETRIES, DELAY = 3, 5
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
            record_call()
            log.warning(f"[{number}] Network error: {e}")
            if attempt < RETRIES - 1:
                time.sleep(DELAY)
                continue
            else:
                break

        record_call()
        if resp.status_code >= 500 and attempt < RETRIES - 1:
            log.warning(f"[{number}] Server error {resp.status_code}, retry {attempt+1}")
            time.sleep(DELAY)
            continue

        try:
            resp.raise_for_status()
            items = resp.json().get('items', [])
        except requests.HTTPError as he:
            log.warning(f"[{number}] HTTP error: {he}")
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

# ─── Main logic for retrying “no directors” ────────────────────────────────────────
def main():
    log.info("Starting retry_no_directors")

    # 1) Load existing directors.json and no_directors.json
    existing_dirs: dict[str, list] = load_json(DIRECTORS_JSON)
    no_directors: dict[str, str] = load_json(NO_DIRECTORS_JSON)
    # no_directors: { "12345678": "2025-02-15", … }

    if not no_directors:
        log.info("no_directors.json is empty; nothing to retry.")
        return

    # 2) Build list of companies to actually attempt now:
    now_date = datetime.utcnow().date()
    to_attempt = []
    for num, first_seen_str in no_directors.items():
        try:
            first_seen = datetime.fromisoformat(first_seen_str).date()
        except ValueError:
            # If parsing fails, treat as “too old”
            first_seen = now_date - timedelta(days=GIVE_UP_DAYS + 1)

        age_days = (now_date - first_seen).days
        if age_days < GIVE_UP_DAYS:
            to_attempt.append(num)
        else:
            log.info(f"[{num}] first-seen {first_seen_str} is >{GIVE_UP_DAYS} days ago; dropping from no_directors.")
            # We’ll drop it after loop

    total = len(to_attempt)
    log.info(f"Retry list: {total} companies (out of {len(no_directors)} total, excluding >{GIVE_UP_DAYS} days old).")

    # 3) Batch‐fetch them
    idx = 0
    while idx < total:
        avail = get_remaining_calls()
        if avail <= 0:
            with _lock:
                wait = 0.1
            time.sleep(max(wait, 0.1))
            continue

        batch_size = min(avail, MAX_WORKERS, total - idx)
        batch = to_attempt[idx : idx + batch_size]
        log.info(f"Dispatching retry batch {idx+1}–{idx+batch_size} of {total}")
        with ThreadPoolExecutor(max_workers=batch_size) as exe:
            future_to_num = {exe.submit(fetch_one, num): num for num in batch}
            for fut in as_completed(future_to_num):
                num, dirs = fut.result()
                if dirs:
                    # Found directors → add to directors.json, remove from no_directors
                    existing_dirs[num] = dirs
                    no_directors.pop(num, None)
                    log.info(f"[{num}] RETRY → fetched {len(dirs)} director(s); moved to directors.json.")
                else:
                    # Still no directors; keep in no_directors.json (timestamp unchanged)
                    log.info(f"[{num}] RETRY → no directors found (still not in Companies House).")

        idx += batch_size

    # 4) Remove any “too old” entries (> GIVE_UP_DAYS) from no_directors
    now_date = datetime.utcnow().date()
    to_remove = []
    for num, first_seen_str in no_directors.items():
        try:
            first_seen = datetime.fromisoformat(first_seen_str).date()
        except ValueError:
            first_seen = now_date - timedelta(days=GIVE_UP_DAYS + 1)

        if (now_date - first_seen).days >= GIVE_UP_DAYS:
            to_remove.append(num)

    for num in to_remove:
        no_directors.pop(num, None)
        log.info(f"[{num}] >{GIVE_UP_DAYS} days since first seen; dropped from no_directors.")

    # 5) Write back JSON files
    save_json = lambda p, d: (
        os.makedirs(os.path.dirname(p), exist_ok=True),
        open(p + ".tmp", "w").write(json.dumps(d, separators=(',', ':'))),
        os.replace(p + ".tmp", p)
    )

    # Save directors.json
    save_json(DIRECTORS_JSON, existing_dirs)
    # Save no_directors.json
    save_json(NO_DIRECTORS_JSON, no_directors)

    log.info(f"Retry cycle complete. directors.json now has {len(existing_dirs)} entries; no_directors.json now has {len(no_directors)} entries.")

if __name__ == '__main__':
    main()
