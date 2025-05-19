#!/usr/bin/env python3
"""
fund_tracker.py

Hourly GH Action that:
- Fetches Companies House data by incorporation date
- Retries on transient errors
- Logs failures
- Always rewrites master_companies.xlsx & assets/data/master_companies.csv
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta

import requests
import pandas as pd

# CONFIG
CH_API_URL     = 'https://api.company-information.service.gov.uk/search/companies'
OUTPUT_EXCEL   = 'master_companies.xlsx'
OUTPUT_CSV     = 'assets/data/master_companies.csv'
LOG_FILE       = 'fund_tracker.log'
RETRY_COUNT    = 3
RETRY_DELAY    = 5
ITEMS_PER_PAGE = 100

# Logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.WARNING)
logger.addHandler(console)

def normalize_date(d: str) -> str:
    """Empty or 'today' → YYYY-MM-DD of today; else return d."""
    if not d or str(d).strip().lower() == 'today':
        return date.today().strftime('%Y-%m-%d')
    return d

def fetch_companies_on(date_str: str, api_key: str) -> list[dict]:
    auth = (api_key, '')
    params = {
        'incorporated_from': date_str,
        'incorporated_to':   date_str,
        'items_per_page':    ITEMS_PER_PAGE
    }
    for attempt in range(1, RETRY_COUNT+1):
        try:
            resp = requests.get(CH_API_URL, auth=auth, params=params, timeout=10)
            if resp.status_code == 200:
                items = resp.json().get('items', [])
                now = datetime.utcnow()
                return [
                    {
                        'Company Name':       c.get('title'),
                        'Company Number':     c.get('company_number'),
                        'Incorporation Date': c.get('date_of_creation'),
                        'Status':             c.get('company_status'),
                        'Source':             c.get('source'),
                        'Date Downloaded':    now.strftime('%Y-%m-%d'),
                        'Time Discovered':    now.strftime('%H:%M:%S')
                    }
                    for c in items
                ]
            else:
                logger.warning(f'Non-200 ({resp.status_code}) on {date_str}, attempt {attempt}')
        except Exception as e:
            logger.warning(f'Error on {date_str}, attempt {attempt}: {e}')
        time.sleep(RETRY_DELAY)
    logger.error(f'Failed to fetch data for {date_str} after {RETRY_COUNT} attempts')
    return []

def run_for_date_range(start_date: str, end_date: str):
    sd = datetime.strptime(start_date, '%Y-%m-%d')
    ed = datetime.strptime(end_date,   '%Y-%m-%d')
    if sd > ed:
        logger.error("start_date cannot be after end_date")
        sys.exit(1)

    records = []
    cur = sd
    while cur <= ed:
        ds = cur.strftime('%Y-%m-%d')
        logger.info(f'Fetching companies for {ds}')
        records.extend(fetch_companies_on(ds, API_KEY))
        cur += timedelta(days=1)

    # Ensure assets/data exists before writing
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    # Always rewrite both files
    df = pd.DataFrame(records)
    df.to_excel(OUTPUT_EXCEL, index=False)
    df.to_csv(OUTPUT_CSV, index=False)
    logger.info(f'Wrote {len(df)} records to {OUTPUT_EXCEL} & {OUTPUT_CSV}')

def main():
    global API_KEY
    p = argparse.ArgumentParser(description='Fetch Companies House data')
    p.add_argument('--start_date', default='', help='YYYY-MM-DD or "today"')
    p.add_argument('--end_date',   default='', help='YYYY-MM-DD or "today"')
    args = p.parse_args()

    API_KEY = os.getenv('CH_API_KEY')
    if not API_KEY:
        logger.error('CH_API_KEY not set')
        sys.exit(1)

    # Normalize empty or "today" values
    sd = normalize_date(args.start_date)
    ed = normalize_date(args.end_date)
    logger.info(f'Starting run: {sd} → {ed}')
    run_for_date_range(sd, ed)

if __name__ == '__main__':
    main()
