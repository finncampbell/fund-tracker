#!/usr/bin/env python3
"""
fetch_directors.py

- Fetches directors for every filtered company in relevant_companies.csv
- Dispatches in dynamic batches sized to remaining API quota
- Honors shared buffered rate limit (600−50)
- Updates docs/assets/data/directors.json
- Logs to assets/logs/director_fetch.log
"""

import os, json, time, argparse, logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from rate_limiter import enforce_rate_limit, record_call, get_remaining_calls

# Config
API_BASE       = 'https://api.company-information.service.gov.uk/company'
CH_KEY         = os.getenv('CH_API_KEY')
RELEVANT_CSV   = 'docs/assets/data/relevant_companies.csv'
DIRECTORS_JSON = 'docs/assets/data/directors.json'
LOG_DIR        = 'assets/logs'
LOG_FILE       = os.path.join(LOG_DIR, 'director_fetch.log')
MAX_WORKERS    = 550

# Logging
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

def load_relevant():
    return pd.read_csv(RELEVANT_CSV, dtype=str)['Company Number'].dropna().tolist()

def load_existing():
    if os.path.exists(DIRECTORS_JSON):
        return json.load(open(DIRECTORS_JSON))
    return {}

def fetch_one(number):
    RETRIES, DELAY = 3, 5
    for i in range(RETRIES):
        enforce_rate_limit()
        resp = requests.get(f"{API_BASE}/{number}/officers", auth=(CH_KEY,''), timeout=10)
        if resp.status_code >= 500 and i < RETRIES-1:
            time.sleep(DELAY); continue
        try:
            resp.raise_for_status()
            record_call()
            items = resp.json().get('items', [])
        except:
            items = []
        break

    ROLES = {'director','member'}
    active = [o for o in items if o.get('officer_role') in ROLES and o.get('resigned_on') is None]
    chosen = active or [o for o in items if o.get('officer_role') in ROLES]

    return number, [{
        'title': o.get('name'),
        'appointment': o.get('snippet',''),
        'dateOfBirth': (f"{o.get('date_of_birth',{}).get('year')}-{int(o.get('date_of_birth',{}).get('month',0)):02d}"
                        if o.get('date_of_birth',{}).get('year') else ''),
        'appointmentCount': o.get('appointment_count'),
        'selfLink': o['links'].get('self'),
        'officerRole': o.get('officer_role'),
        'nationality': o.get('nationality'),
        'occupation': o.get('occupation')
    } for o in chosen]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', help='unused', default='')
    parser.add_argument('--end_date',   help='unused', default='')
    args = parser.parse_args()

    log.info("Starting fetch_directors")
    existing = load_existing()
    relevant = load_relevant()
    pending  = [n for n in relevant if n not in existing]
    total    = len(pending); idx = 0

    while idx < total:
        avail = get_remaining_calls()
        if avail <= 0:
            time.sleep(1); continue
        batch = pending[idx:idx+avail]
        log.info(f"Dispatching batch {idx+1}-{idx+len(batch)} of {total}")
        with ThreadPoolExecutor(max_workers=len(batch)) as exe:
            for fut in as_completed({exe.submit(fetch_one, num): num for num in batch}):
                num, dirs = fut.result()
                existing[num] = dirs
                log.info(f"Fetched {len(dirs)} directors for {num}")
        idx += len(batch)

    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
    json.dump(existing, open(DIRECTORS_JSON,'w'), separators=(',',':'))
    log.info(f"Wrote directors.json with {len(existing)} entries")

if __name__ == '__main__':
    main()
