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
CH_API_URL       = 'https://api.company-information.service.gov.uk/advanced-search/companies'
OUTPUT_EXCEL     = 'master_companies.xlsx'
OUTPUT_CSV       = 'assets/data/master_companies.csv'
LOG_FILE         = 'fund_tracker.log'
RETRY_COUNT      = 3
RETRY_DELAY      = 5   # seconds
FETCH_SIZE       = 100 # advanced-search uses 'size'
# ───────────────────────────────────────────────────────────────────────────────

# Set up logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)
# also show warnings/errors in the Actions console
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.WARNING)
logger.addHandler(console)


def normalize_date(d: str) -> str:
    """
    If d is empty or 'today' (any case), return today's date (YYYY-MM-DD).
    Otherwise return d unchanged.
    """
    if not d or d.strip().lower() == 'today':
        return date.today().strftime('%Y-%m-%d')
    return d


def fetch_companies_on(date_str: str, api_key: str) -> list[dict]:
    """
    Fetch up to FETCH_SIZE companies incorporated on date_str.
    Uses the advanced-search endpoint with 'size', retries on transient errors.
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
        except requests.RequestException as e:
            logger.warning(f'Error on {date_str}, attempt {attempt}: {e}')
        time.sleep(RETRY_DELAY)

    logger.error(f'Failed to fetch data for {date_str} after {RETRY_COUNT} attempts')
    return []


def run_for_date_range(start_date: str, end_date: str):
    """
    Fetch for each day in range (inclusive), then append only new rows to master.
    """
    sd = datetime.strptime(start_date, '%Y-%m-%d')
    ed = datetime.strptime(end_date,   '%Y-%m-%d')
    if sd > ed:
        logger.error("start_date cannot be after end_date")
        sys.exit(1)

    # Gather today’s (or requested range’s) new records
    new_records = []
    current = sd
    while current <= ed:
        ds = current.strftime('%Y-%m-%d')
        logger.info(f'Fetching companies for {ds}')
        new_records.extend(fetch_companies_on(ds, API_KEY))
        current += timedelta(days=1)

    # Ensure the assets/data folder exists
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    # Load existing master CSV (or start empty)
    if os.path.exists(OUTPUT_CSV):
        df_master = pd.read_csv(OUTPUT_CSV)
    else:
        df_master = pd.DataFrame(columns=[
            'Company Name','Company Number','Incorporation Date',
            'Status','Source','Date Downloaded','Time Discovered'
        ])

    if new_records:
        df_new = pd.DataFrame(new_records)
        # Combine, dedupe on Company Number (keep existing first)
        df = pd.concat([df_master, df_new], ignore_index=True)
        df.drop_duplicates(subset=['Company Number'], keep='first', inplace=True)
        df.sort_values('Incorporation Date', ascending=False, inplace=True)

        # Write updated master files
        df.to_excel(OUTPUT_EXCEL, index=False)
        df.to_csv(OUTPUT_CSV, index=False)
        logger.info(f'Appended {len(df_new)} new rows; master now has {len(df)} records')
    else:
        logger.info('No new records to append')


def main():
    global API_KEY
    parser = argparse.ArgumentParser(description='Fetch Companies House data')
    parser.add_argument('--start_date', default='', help='YYYY-MM-DD or "today"')
    parser.add_argument('--end_date',   default='', help='YYYY-MM-DD or "today"')
    args = parser.parse_args()

    API_KEY = os.getenv('CH_API_KEY')
    if not API_KEY:
        logger.error('CH_API_KEY environment variable is not set')
        sys.exit(1)

    # Normalize and log
    sd = normalize_date(args.start_date)
    ed = normalize_date(args.end_date)
    logger.info(f'Starting run: {sd} → {ed}')

    run_for_date_range(sd, ed)


if __name__ == '__main__':
    main()
