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
  sorted with newest downloads first.
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
RETRY_DELAY      = 5    # seconds between retries
FETCH_SIZE       = 100  # number of items to fetch per request
# ───────────────────────────────────────────────────────────────────────────────

# Set up logging
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
    """Empty or 'today' (case-insensitive) → today's date (YYYY-MM-DD); else return d."""
    if not d or d.strip().lower() == 'today':
        return date.today().strftime('%Y-%m-%d')
    return d


def fetch_companies_on(date_str: str, api_key: str) -> list[dict]:
    """
    Fetch up to FETCH_SIZE companies incorporated on date_str. Retries on transient errors.
    Returns list of dicts with guaranteed 'Company Name' key.
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
                records = []
                for c in items:
                    # fallback on possible name fields
                    name = c.get('title') or c.get('company_name') or c.get('companyName') or ''
                    records.append({
                        'Company Name':       name,
                        'Company Number':     c.get('company_number', ''),
                        'Incorporation Date': c.get('date_of_creation', ''),
                        'Status':             c.get('company_status', ''),
                        'Source':             c.get('source', ''),
                        'Date Downloaded':    now.strftime('%Y-%m-%d'),
                        'Time Discovered':    now.strftime('%H:%M:%S')
                    })
                return records
            else:
                logger.warning(f'Non-200 ({resp.status_code}) for {date_str}, attempt {attempt}')
        except requests.RequestException as e:
            logger.warning(f'Error on {date_str}, attempt {attempt}: {e}')
        time.sleep(RETRY_DELAY)

    logger.error(f'Failed to fetch data for {date_str} after {RETRY_COUNT} attempts')
    return []


def run_for_date_range(start_date: str, end_date: str):
    """
    Fetch for each day in range, then append and write master files.
    """
    sd = datetime.strptime(start_date, '%Y-%m-%d')
    ed = datetime.strptime(end_date,   '%Y-%m-%d')
    if sd > ed:
        logger.error("start_date cannot be after end_date")
        sys.exit(1)

    # 1) Collect new records
    new_records = []
    current = sd
    while current <= ed:
        ds = current.strftime('%Y-%m-%d')
        logger.info(f'Fetching companies for {ds}')
        batch = fetch_companies_on(ds, API_KEY)
        logger.info(f' → fetched {len(batch)} items')
        new_records.extend(batch)
        current += timedelta(days=1)

    # 2) Ensure output folder exists
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    # 3) Load existing master or create empty
    if os.path.exists(OUTPUT_CSV):
        df_master = pd.read_csv(OUTPUT_CSV)
    else:
        df_master = pd.DataFrame(columns=[
            'Company Name','Company Number','Incorporation Date',
            'Status','Source','Date Downloaded','Time Discovered'
        ])

    # 4) Append & dedupe
    if new_records:
        # Log a sample of new_records for debugging
        logger.info(f"Sample new record: {new_records[0] if new_records else 'none'}")
        df_new = pd.DataFrame(new_records)
        df_all = pd.concat([df_master, df_new], ignore_index=True)
        df_all.drop_duplicates(subset=['Company Number'], keep='first', inplace=True)
    else:
        df_all = df_master
        logger.info('No new records to append')

    # 5) Sort: newest downloads, then times, then incorporation date
    df_all.sort_values(
        by=['Date Downloaded', 'Time Discovered', 'Incorporation Date'],
        ascending=[False, False, False],
        inplace=True
    )
    df_all.reset_index(drop=True, inplace=True)

    # 6) Write both outputs
    df_all.to_excel(OUTPUT_EXCEL, index=False)
    df_all.to_csv(OUTPUT_CSV, index=False)
    logger.info(f'Master updated: {len(df_all)} total records (added {len(new_records)})')


def main():
    global API_KEY
    parser = argparse.ArgumentParser(description='Fetch Companies House data')
    parser.add_argument('--start_date', default='', help='YYYY-MM-DD or "today"')
    parser.add_argument('--end_date',   default='', help='YYYY-MM-DD or "today"')
    args = parser.parse_args()

    API_KEY = os.getenv('CH_API_KEY')
    if not API_KEY:
        logger.error('Environment variable CH_API_KEY is not set')
        sys.exit(1)

    sd = normalize_date(args.start_date)
    ed = normalize_date(args.end_date)
    logger.info(f'Starting run: {sd} → {ed}')
    run_for_date_range(sd, ed)


if __name__ == '__main__':
    main()
