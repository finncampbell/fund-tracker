#!/usr/bin/env python3
"""
fetch_directors.py

- Fetches only new companies’ directors
- Writes directly to docs/assets/data/directors.json
"""

import os
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from rate_limiter import enforce_rate_limit, record_call

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
API_BASE       = 'https://api.company-information.service.gov.uk/company'
CH_KEY         = os.getenv('CH_API_KEY')
RELEVANT_CSV   = 'docs/assets/data/relevant_companies.csv'
DIRECTORS_JSON = 'docs/assets/data/directors.json'
LOG_FILE       = 'director_fetch.log'
MAX_WORKERS    = 10
MAX_PENDING    = 50
RETRIES        = 3
RETRY_DELAY    = 5

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log=logging.getLogger(__name__)
os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)

def load_relevant():
    df=pd.read_csv(RELEVANT_CSV,dtype=str)
    return [n for n in df['Company Number'].dropna().unique()]

def load_existing():
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON) as f:
            return json.load(f)
    return {}

def fetch_one(num):
    items=[]
    for attempt in range(1,RETRIES+1):
        enforce_rate_limit()
        resp=requests.get(f"{API_BASE}/{num}/officers",
                          auth=(CH_KEY,''),timeout=10)
        if resp.status_code>=500 and attempt<RETRIES:
            log.warning(f"{num}: {resp.status_code}, retrying")
            time.sleep(RETRY_DELAY)
            continue
        try:
            resp.raise_for_status()
            record_call()
            items=resp.json().get('items',[])
        except Exception as e:
            log.warning(f"{num}: fetch error {e}")
        break

    ROLES={'director','member'}
    active=[o for o in items if o.get('officer_role') in ROLES and o.get('resigned_on') is None]
    chosen=active or [o for o in items if o.get('officer_role') in ROLES]

    directors=[]
    for o in chosen:
        dob=o.get('date_of_birth') or {}
        y,m=dob.get('year'),dob.get('month')
        dob_str=f"{y}-{int(m):02d}" if y and m else str(y) if y else ''
        directors.append({
            'title':        o.get('name'),
            'appointment':  o.get('snippet',''),
            'dateOfBirth':  dob_str,
            'appointmentCount': o.get('appointment_count'),
            'selfLink':     o['links'].get('self'),
            'officerRole':  o.get('officer_role'),
            'nationality':  o.get('nationality'),
            'occupation':   o.get('occupation'),
        })
    return num, directors

def main():
    log.info("Starting fetch_directors")
    existing=load_existing()
    log.info(f"Loaded {len(existing)} existing entries")
    relevant=load_relevant()
    pending=[n for n in relevant if n not in existing][:MAX_PENDING]
    log.info(f"{len(pending)} pending to fetch")

    if pending:
        with ThreadPoolExecutor(MAX_WORKERS) as exe:
            futures={exe.submit(fetch_one,n):n for n in pending}
            for fut in as_completed(futures):
                num,dirs = fut.result()
                existing[num]=dirs
                log.info(f"Fetched {len(dirs)} for {num}")

    with open(DIRECTORS_JSON,'w') as f:
        json.dump(existing,f,separators=(',',':'))
    log.info(f"Wrote {len(existing)} entries to directors.json")

if __name__=='__main__':
    if not CH_KEY:
        log.error("CH_API_KEY unset"); exit(1)
    main()
