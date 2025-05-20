# fund_tracker.py
#!/usr/bin/env python3
"""
fund_tracker.py

- Fetches Companies House data by incorporation date (default: today)
- Retries on transient errors
- Logs to fund_tracker.log
- Builds two outputs:
  • master_companies.csv / .xlsx  (full history, with Category)
  • relevant_companies.csv / .xlsx (only matching Categories)
Each file always includes the full header row in the same order.
"""

import argparse
import logging
import os
import sys
import time
import re
from datetime import date, datetime, timedelta

import requests
import pandas as pd

# CONFIGURATION
CH_API_URL    = 'https://api.company-information.service.gov.uk/advanced-search/companies'
MASTER_XLSX   = 'master_companies.xlsx'
MASTER_CSV    = 'assets/data/master_companies.csv'
RELEVANT_XLSX = 'relevant_companies.xlsx'
RELEVANT_CSV  = 'assets/data/relevant_companies.csv'
LOG_FILE      = 'fund_tracker.log'
RETRY_COUNT   = 3
RETRY_DELAY   = 5     # seconds
FETCH_SIZE    = 100   # items per request

# Column order for all CSV/XLSX outputs
FIELDS = [
    'Company Name',
    'Company Number',
    'Incorporation Date',
    'Status',
    'Source',
    'Date Downloaded',
    'Time Discovered',
    'Category'
]

# Patterns for classification, in priority order
KEYWORD_PATTERNS = [
    ('LLP',         r'\bL\W*L\W*P\b'),
    ('LP',          r'\bL\W*P\b'),
    ('GP',          r'\bG\W*P\b'),
    ('Fund',        r'\bF\W*U\W*N\W*D\b'),
    ('Ventures',    r'\bVentures\b'),
    ('Capital',     r'\bCapital\b'),
    ('Equity',      r'\bEquity\b'),
    ('Advisors',    r'\bAdvisors\b'),
    ('Partners',    r'\bPartners\b'),
    ('SIC',         r'\bSIC\b'),
    ('Investments', r'\bInvestments\b'),
]

# Set up logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.WARNING)
log.addHandler(console)

def normalize_date(d: str) -> str:
    """Empty or 'today' → today’s date; else return as-is."""
    if not d or d.lower() == 'today':
        return date.today().strftime('%Y-%m-%d')
    return d

def classify(name: str) -> str:
    """Return first matching keyword or 'Other'."""
    name = name or ''
    for kw, pattern in KEYWORD_PATTERNS:
        if re.search(pattern, name, flags=re.IGNORECASE):
            return kw
    return 'Other'

def fetch_companies_on(date_str: str, api_key: str) -> list[dict]:
    """Hit the advanced-search endpoint, retry on failure."""
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
                now = datetime.utcnow()
                recs = []
                for c in resp.json().get('items', []):
                    nm = c.get('title') or c.get('company_name') or ''
                    recs.append({
                        'Company Name':       nm,
                        'Company Number':     c.get('company_number',''),
                        'Incorporation Date': c.get('date_of_creation',''),
                        'Status':             c.get('company_status',''),
                        'Source':             c.get('source',''),
                        'Date Downloaded':    now.strftime('%Y-%m-%d'),
                        'Time Discovered':    now.strftime('%H:%M:%S'),
                        'Category':           classify(nm)
                    })
                return recs
            else:
                log.warning(f'Non-200 ({resp.status_code}) on {date_str}, attempt {attempt}')
        except Exception as e:
            log.warning(f'Error on {date_str}, attempt {attempt}: {e}')
        time.sleep(RETRY_DELAY)

    log.error(f'Failed to fetch for {date_str}')
    return []

def run_for_date_range(start_date: str, end_date: str):
    """Fetch each day, append & dedupe master, then write master + relevant files."""
    sd = datetime.strptime(start_date, '%Y-%m-%d')
    ed = datetime.strptime(end_date,   '%Y-%m-%d')
    if sd > ed:
        log.error("start_date cannot be after end_date")
        sys.exit(1)

    new_records = []
    cur = sd
    while cur <= ed:
        ds = cur.strftime('%Y-%m-%d')
        log.info(f'Fetching companies for {ds}')
        new_records.extend(fetch_companies_on(ds, API_KEY))
        cur += timedelta(days=1)

    os.makedirs(os.path.dirname(MASTER_CSV), exist_ok=True)

    # Load or init master DataFrame
    if os.path.exists(MASTER_CSV):
        df_master = pd.read_csv(MASTER_CSV)
    else:
        df_master = pd.DataFrame(columns=FIELDS)

    # Append, dedupe
    if new_records:
        df_new = pd.DataFrame(new_records, columns=FIELDS)
        df_all = pd.concat([df_master, df_new], ignore_index=True)
        df_all.drop_duplicates(subset=['Company Number'], keep='first', inplace=True)
    else:
        df_all = df_master
        log.info('No new records to append')

    # Sort by incorporation date descending & enforce column order
    df_all.sort_values('Incorporation Date', ascending=False, inplace=True)
    df_all = df_all[FIELDS]

    # Write master outputs
    df_all.to_excel(MASTER_XLSX, index=False)
    df_all.to_csv(MASTER_CSV, index=False)
    log.info(f'Master file updated: {len(df_all)} rows')

    # Filter relevant (Category != Other) and write
    df_rel = df_all[df_all['Category'] != 'Other']
    df_rel.to_excel(RELEVANT_XLSX, index=False)
    df_rel.to_csv(RELEVANT_CSV, index=False)
    log.info(f'Relevant file updated: {len(df_rel)} rows')

def main():
    global API_KEY
    parser = argparse.ArgumentParser(description='Fetch and classify CH data')
    parser.add_argument('--start_date', default='', help='YYYY-MM-DD or "today"')
    parser.add_argument('--end_date',   default='', help='YYYY-MM-DD or "today"')
    args = parser.parse_args()

    API_KEY = os.getenv('CH_API_KEY')
    if not API_KEY:
        log.error('CH_API_KEY environment variable not set')
        sys.exit(1)

    sd = normalize_date(args.start_date)
    ed = normalize_date(args.end_date)
    log.info(f'Starting run: {sd} → {ed}')
    run_for_date_range(sd, ed)

if __name__ == '__main__':
    main()
