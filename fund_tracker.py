#!/usr/bin/env python3
"""
fund_tracker.py
...
"""

import argparse
import sys
import time
from datetime import date, datetime, timedelta, timezone

import re
import requests
import pandas as pd

from rate_limiter import enforce_rate_limit, record_call
from logger import log  # ← centralized logger

# ─── Configuration ─────────────────────────────────────────────────────────────
CH_API_URL    = 'https://api.company-information.service.gov.uk/advanced-search/companies'
MASTER_CSV    = 'assets/data/master_companies.csv'
MASTER_XLSX   = 'assets/data/master_companies.xlsx'
RELEVANT_CSV  = 'assets/data/relevant_companies.csv'
RELEVANT_XLSX = 'assets/data/relevant_companies.xlsx'
FETCH_SIZE    = 100
RETRY_COUNT   = 3
RETRY_DELAY   = 5  # seconds

# ─── SIC Lookup & Fields ───────────────────────────────────────────────────────
SIC_LOOKUP = {
    '64205': ("Activities of financial services holding companies",
              "Holding-company SPV for portfolio-company equity stakes, co-investment vehicles, master/feeder hubs."),
    # … other entries …
    '70221': ("Financial management (of companies and enterprises)",
              "Treasury, capital-raising and internal financial services arm.")
}
FIELDS = [
    'Company Name','Company Number','Incorporation Date',
    'Status','Source','Date Downloaded','Time Discovered',
    'Category','SIC Codes','SIC Description','Typical Use Case'
]

# ─── Classification Patterns ───────────────────────────────────────────────────
CLASS_PATTERNS = [
    # same as before…
]

def normalize_date(d: str) -> str:
    # same as before…
    # Use log for errors
    if not d or d.lower() == 'today':
        return date.today().strftime('%Y-%m-%d')
    try:
        return datetime.strptime(d, '%Y-%m-%d').strftime('%Y-%m-%d')
    except ValueError:
        pass
    try:
        return datetime.strptime(d, '%d-%m-%Y').strftime('%Y-%m-%d')
    except ValueError:
        log.error(f"Invalid date format: {d}. Expected YYYY-MM-DD or DD-MM-YYYY")
        sys.exit(1)

# … classify(), enrich_sic(), fetch_companies_on() as before, using log instead of log.warning/log.info …

def run_for_date_range(start_date: str, end_date: str):
    sd = datetime.strptime(start_date, '%Y-%m-%d')
    ed = datetime.strptime(end_date,   '%Y-%m-%d')
    if sd > ed:
        log.error("start_date cannot be after end_date")
        sys.exit(1)

    log.info(f"Starting company ingest {start_date} → {end_date}")
    # … rest unchanged, using log.info() …

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--start_date', default='', help='YYYY-MM-DD or "today"')
    p.add_argument('--end_date',   default='', help='YYYY-MM-DD or "today"')
    args = p.parse_args()

    api_key = os.getenv('CH_API_KEY')
    if not api_key:
        log.error('CH_API_KEY not set')
        sys.exit(1)

    sd = normalize_date(args.start_date)
    ed = normalize_date(args.end_date)
    run_for_date_range(sd, ed)

if __name__ == '__main__':
    main()
