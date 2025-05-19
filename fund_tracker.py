#!/usr/bin/env python3
"""
fund_tracker.py

Hourly GitHub Action that:
- Fetches Companies House data by incorporation date (default: today)
- Retries on transient errors
- Logs failures to fund_tracker.log
- Appends new rows to:
    • master_companies.xlsx
    • assets/data/master_companies.csv
  then sorts everything by Incorporation Date (newest first).
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta

import requests
import pandas as pd

# ─────── CONFIGURATION ─────────────────────────────────────────────────────────
CH_API_URL     = 'https://api.company-information.service.gov.uk/advanced-search/companies'
OUTPUT_EXCEL   = 'master_companies.xlsx'
OUTPUT_CSV     = 'assets/data/master_companies.csv'
LOG_FILE       = 'fund_tracker.log'
RETRY_COUNT    = 3
RETRY_DELAY    = 5    # seconds between retries
FETCH_SIZE     = 100  # number of items per request
# ───────────────────────────────────────────────────────────────────────────────

# Set up logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)
# Also log warnings/errors to stdout for Action visibility
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.WARNING)
logger.addHandler(console)


def normalize_date(d: str) -> str:
    """Empty or 'today' → today's date; else return d."""
    if not d or d.strip().lower() == 'today':
        return date.today().strftime('%Y-%m-%d')
    return d


def fetch_companies_on(date_str: str, api_key: str) -> list[dict]:
    """
    Fetch up to FETCH_SIZE companies created on date_str.
    Retries on failures. Returns list of dicts.
    """
    auth = (api_key, '')
    params = {
        'incorporated_from': date_str,
        'incorporated_to':   date_str,
        'size':              FETCH_SIZE
    }

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(CH_API_URL, auth=auth, params=params, timeout=10)
            if resp.status_code == 200:
                items = resp.json().get('items', [])
                now = datetime.utcnow()
                return [
                    {
                        'Company Name':       c.get('title', ''),
                        'Company Number':     c.get('company_number', ''),
                        'Incorporation Date': c.get('date_of_creation', ''),
                        'Status':             c.get('company_status', ''),
                        'Source':             c.get('source', ''),
                        'Date Downloaded':    now.strftime('%Y-%m-%d'),
                        'Time Discovered':    now.strftime('%H:%M:%S')
                    }
                    for c in items
                ]
            else:
                logger.warning(f'Non-200 ({resp.status_code}) on {date_str}, attempt {attempt}')
        except requests.RequestException as e:
            logger.warning(f'Error on {date_str}, attempt {attempt}: {e}')
        time.sleep(RETRY_DELAY)

    logger.error(f'Failed to fetch data for {date_str} after {RETRY_COUNT} attempts')
    return []


def run_for_date_range(start_date: str, end_date: str):
    """
    Fetch companies for each day in [start_date, end_date], then
    append to master, dedupe, sort by Incorporation Date desc, and write.
    """
    sd = datetime.strptime(start_date, '%Y-%m-%d')
    ed = datetime.strptime(end_date,   '%Y-%m-%d')
    if sd > ed:
        logger.error("start_date cannot be after end_date")
        sys.exit(1)

    all_new = []
    current = sd
    while current <= ed:
        ds = current.strftime('%Y-%m-%d')
        logger.info(f'Fetching companies on {ds}')
        batch = fetch_companies_on(ds, API_KEY)
        logger.info(f' → fetched {len(batch)} records')
        all_new.extend(batch)
        current += timedelta(days=1)

    # Ensure output dir exists
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    # Load existing
    if os.path.exists(OUTPUT_CSV):
        df_master = pd.read_csv(OUTPUT_CSV)
    else:
        df_master = pd.DataFrame(columns=[
            'Company Name','Company Number','Incorporation Date',
            'Status','Source','Date Downloaded','Time Discovered'
        ])

    # Append & dedupe
    if all_new:
        df_new = pd.DataFrame(all_new)
        df_master = pd.concat([df_master, df_new], ignore_index=True)
        df_master.drop_duplicates(subset=['Company Number'], keep='first', inplace=True)
    else:
        logger.info('No new records to append')

    # Sort by Incorporation Date (newest first)
    df_master.sort_values('Incorporation Date', ascending=False, inplace=True)
    df_master.reset_index(drop=True, inplace=True)

    # Write out both files
    df_master.to_excel(OUTPUT_EXCEL, index=False)
    df_master.to_csv(OUTPUT_CSV, index=False)
    logger.info(f'Wrote {len(df_master)} total records to Excel & CSV')


def main():
    global API_KEY
    parser = argparse.ArgumentParser(description='Fetch CH data by date')
    parser.add_argument('--start_date', default='', help='YYYY-MM-DD or "today"')
    parser.add_argument('--end_date',   default='', help='YYYY-MM-DD or "today"')
    args = parser.parse_args()

    API_KEY = os.getenv('CH_API_KEY')
    if not API_KEY:
        logger.error('CH_API_KEY not set')
        sys.exit(1)

    sd = normalize_date(args.start_date)
    ed = normalize_date(args.end_date)
    logger.info(f'Starting: {sd} → {ed}')
    run_for_date_range(sd, ed)


if __name__ == '__main__':
    main()
