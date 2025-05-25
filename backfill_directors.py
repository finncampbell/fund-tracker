#!/usr/bin/env python3
"""
backfill_directors.py

- Historical backfill to docs/assets/data/directors.json
"""

import os
import json
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import requests
from rate_limiter import enforce_rate_limit, record_call

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
    return pd.read_csv(RELEVANT_CSV,dtype=str,parse_dates=['Incorporation Date'])

def load_existing():
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON) as f: return json.load(f)
    return {}

def fetch_officers(num):
    # identical to fetch_one in fetch_directors.py
    ...

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--start_date',required=True)
    parser.add_argument('--end_date',required=True)
    args=parser.parse_args()

    df=load_relevant(); existing=load_existing()
    sd=datetime.fromisoformat(args.start_date).date()
    ed=datetime.fromisoformat(args.end_date).date()

    mask_pending=~df['Company Number'].isin(existing.keys())
    mask_hist=df['Incorporation Date'].dt.date.between(sd,ed)
    pending=df[mask_pending&mask_hist]['Company Number'].tolist()[:MAX_PENDING]
    log.info(f"{len(pending)} to backfill")

    if pending:
        with ThreadPoolExecutor(MAX_WORKERS) as exe:
            futures={exe.submit(fetch_officers,n):n for n in pending}
            for fut in as_completed(futures):
                num,dirs=fut.result()
                existing[num]=dirs
                log.info(f"Backfilled {len(dirs)} for {num}")

    with open(DIRECTORS_JSON,'w') as f:
        json.dump(existing,f,separators=(',',':'))
    log.info(f"Wrote {len(existing)} to directors.json")

if __name__=='__main__':
    if not CH_KEY: log.error("CH_API_KEY unset"); exit(1)
    main()
