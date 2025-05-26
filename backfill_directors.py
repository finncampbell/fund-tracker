#!/usr/bin/env python3
"""
backfill_directors.py

- Historical backfill within start–end date
- Emits backfill_status.json for UI progress
- Dynamically batches up to remaining API quota
- Updates docs/assets/data/directors.json
- Logs to assets/logs/backfill_directors.log
"""

import os, json, time, argparse, logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from rate_limiter import enforce_rate_limit, record_call, get_remaining_calls
from logger import get_logger

API_BASE       = 'https://api.company-information.service.gov.uk/company'
CH_KEY         = os.getenv('CH_API_KEY')
RELEVANT_CSV   = 'docs/assets/data/relevant_companies.csv'
DIRECTORS_JSON = 'docs/assets/data/directors.json'
STATUS_FILE    = 'docs/assets/data/backfill_status.json'
LOG_DIR        = 'assets/logs'
LOG_FILE       = os.path.join(LOG_DIR, 'backfill_directors.log')
MAX_WORKERS    = 550

os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
log = get_logger('backfill_directors', LOG_FILE)

def write_status(total, processed):
    os.makedirs(os.path.dirname(STATUS_FILE), exist_ok=True)
    json.dump({'total': total, 'processed': processed},
              open(STATUS_FILE,'w'))

def load_relevant():
    df = pd.read_csv(RELEVANT_CSV, dtype=str, parse_dates=['Incorporation Date'])
    return df

def load_existing():
    if os.path.exists(DIRECTORS_JSON):
        return json.load(open(DIRECTORS_JSON))
    return {}

def fetch_officers(number):
    RETRIES, DELAY = 3, 5
    for i in range(RETRIES):
        enforce_rate_limit()
        resp = requests.get(f"{API_BASE}/{number}/officers", auth=(CH_KEY,''), params={'register_view':'true'}, timeout=10)
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
    parser.add_argument('--start_date', required=True)
    parser.add_argument('--end_date',   required=True)
    args = parser.parse_args()

    df       = load_relevant()
    existing = load_existing()

    sd = datetime.fromisoformat(args.start_date).date()
    ed = datetime.fromisoformat(args.end_date).date()
    mask_new = ~df['Company Number'].isin(existing.keys())
    mask_win = df['Incorporation Date'].dt.date.between(sd, ed)
    pending  = df[mask_new & mask_win]['Company Number'].tolist()

    total, processed = len(pending), 0
    write_status(total, processed)
    log.info(f"Backfill start: {total} companies")

    idx = 0
    while idx < total:
        avail = get_remaining_calls()
        if avail <= 0:
            time.sleep(1); continue
        batch = pending[idx:idx+min(avail, MAX_WORKERS)]
        log.info(f"Dispatching batch {idx+1}-{idx+len(batch)} of {total}")
        with ThreadPoolExecutor(max_workers=len(batch)) as exe:
            for fut in as_completed({exe.submit(fetch_officers, num): num for num in batch}):
                num, dirs = fut.result()
                existing[num] = dirs
                processed += 1
                write_status(total, processed)
                log.info(f"Fetched {len(dirs)} officers for {num} ({processed}/{total})")
        idx += len(batch)

    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
    json.dump(existing, open(DIRECTORS_JSON,'w'), separators=(',',':'))
    log.info(f"Wrote directors.json with {len(existing)} entries")

if __name__ == '__main__':
    main()
