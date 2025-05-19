#!/usr/bin/env python3
"""
fund_tracker.py

Hourly GitHub Action that:
- Fetches Companies House data by incorporation date
- Retries on transient errors
- Logs failures to fund_tracker.log
- Writes successes to:
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
CH_API_URL       = 'https://api.company-information.service.gov.uk/search/companies'
OUTPUT_EXCEL     = 'master_companies.xlsx'
OUTPUT_CSV       = 'assets/data/master_companies.csv'
LOG_FILE         = 'fund_tracker.log'
RETRY_COUNT      = 3
RETRY_DELAY      = 5  # seconds between retries
ITEMS_PER_PAGE   = 100
# ───────────────────────────────────────────────────────────────────────────────

# Set up logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)
# also log warnings/errors to console for CI visibility
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
    Fetch up to ITEMS_PER_PAGE companies incorporated on date_str.
    Retries RETRY_COUNT times on network errors or non-200 responses.
    Returns list of normalized dicts or empty list on permanent failure.
    """
    auth = (api_key, '')  # Company House uses HTTP Basic Auth: key as user
    params = {
        'incorporated_from': date_str,
        'incorporated_to':   date_str,
        'items_per_page':    ITEMS_PER_PAGE
    }

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(CH_API_URL, auth=auth, params=params, timeout=10)
            if resp.status_code == 200:
                payload = resp.json().get('items', [])
                return [
                    {
                        'Company Name':       c.get('title'),
                        'Company Number':     c.get('company_number'),
                        'Incorporation Date': c.get('date_of_creation'),
                        'Status':             c.get('company_status'),
                        'Source':             c.get('source'),
                        'Date Downloaded':    datetime.utcnow().strftime('%Y-%m-%d'),
                        'Time Discovered':    datetime.utcnow().strftime('%H:%M:%S')
                    }
                    for c in payload
                ]
            else:
                logger.warning(
                    f'Non-200 ({resp.status_code}) for {date_str}, attempt {attempt}'
                )
        except requests.RequestException as e:
            logger.warning(f'Error on {date_str}, attempt {attempt}: {e!r}')
        time.sleep(RETRY_DELAY)

    logger.error(f'Failed to fetch data for {date_str} after {RETRY_COUNT} attempts')
    return []


def run_for_date_range(start_date: str, end_date: str):
    """
    Iterate from start_date to end_date (inclusive), fetch companies each day,
    then write combined results to Excel and CSV.
    """
    sd = datetime.strptime(start_date, '%Y-%m-%d')
    ed = datetime.strptime(end_date,   '%Y-%m-%d')

    # validate range
    if sd > ed:
        logger.error("start_date cannot be after end_date")
        sys.exit(1)

    all_records = []
    current = sd
    while current <= ed:
        ds = current.strftime('%Y-%m-%d')
        logger.info(f'Fetching companies for {ds}')
        records = fetch_companies_on(ds, API_KEY)
        all_records.extend(records)
        current += timedelta(days=1)

    if all_records:
        # ensure output folder exists
        os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

        df = pd.DataFrame(all_records)
        df.to_excel(OUTPUT_EXCEL, index=False)
        df.to_csv(OUTPUT_CSV, index=False)
        logger.info(f'Wrote {len(all_records)} records to {OUTPUT_EXCEL} & {OUTPUT_CSV}')
    else:
        logger.info('No records found for the given date range')


def main():
    global API_KEY
    parser = argparse.ArgumentParser(
        description='Fetch Companies House data by incorporation date'
    )
    parser.add_argument(
        '--start_date',
        default='',
        help='YYYY-MM-DD or "today"'
    )
    parser.add_argument(
        '--end_date',
        default='',
        help='YYYY-MM-DD or "today"'
    )
    args = parser.parse_args()

    # Read API key
    API_KEY = os.getenv('CH_API_KEY')
    if not API_KEY:
        logger.error('Environment variable CH_API_KEY is not set')
        sys.exit(1)

    # Normalize user inputs
    start_date = normalize_date(args.start_date)
    end_date   = normalize_date(args.end_date)
    logger.info(f'Starting run: {start_date} → {end_date}')

    run_for_date_range(start_date, end_date)


if __name__ == '__main__':
    main()
