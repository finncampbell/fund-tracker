#!/usr/bin/env python3
"""
fund_tracker.py

- Fetches Companies House data by incorporation date (default: today)
- Classifies via regex against keywords and SIC lookup
- Paginates through all results (100 per page)
- Writes master_companies.csv/.xlsx (full history)
- Writes relevant_companies.csv/.xlsx (filtered subset)
- Uses openpyxl for XLSX output
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import re
import requests
import pandas as pd
from rate_limiter import enforce_rate_limit, record_call

# ─── Configuration ─────────────────────────────────────────────────────────────
CH_API_URL    = 'https://api.company-information.service.gov.uk/advanced-search/companies'
MASTER_CSV    = 'assets/data/master_companies.csv'
MASTER_XLSX   = 'assets/data/master_companies.xlsx'
RELEVANT_CSV  = 'assets/data/relevant_companies.csv'
RELEVANT_XLSX = 'assets/data/relevant_companies.xlsx'
LOG_FILE      = 'fund_tracker.log'
FETCH_SIZE    = 100
RETRY_COUNT   = 3
RETRY_DELAY   = 5  # seconds

# ─── Logging Setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)
os.makedirs(os.path.dirname(MASTER_CSV), exist_ok=True)

# ─── SIC Lookup & Fields ───────────────────────────────────────────────────────
SIC_LOOKUP = {
    '64205': ("Activities of financial services holding companies",
              "Holding-company SPV for portfolio-company equity stakes, co-investment vehicles, master/feeder hubs."),
    # … your other SIC_LOOKUP entries …
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
    (re.compile(r'\bL[\.\-\s]?L[\.\-\s]?P\b', re.IGNORECASE), 'LLP'),
    (re.compile(r'\bL[\.\-\s]?P\b',           re.IGNORECASE), 'LP'),
    (re.compile(r'\bG[\.\-\s]?P\b',           re.IGNORECASE), 'GP'),
    (re.compile(r'\bFund\b',                  re.IGNORECASE), 'Fund'),
    (re.compile(r'\bVentures?\b',             re.IGNORECASE), 'Ventures'),
    (re.compile(r'\bInvestment(s)?\b',        re.IGNORECASE), 'Investments'),
    (re.compile(r'\bCapital\b',               re.IGNORECASE), 'Capital'),
    (re.compile(r'\bEquity\b',                re.IGNORECASE), 'Equity'),
    (re.compile(r'\bAdvisors\b',              re.IGNORECASE), 'Advisors'),
    (re.compile(r'\bPartners\b',              re.IGNORECASE), 'Partners'),
    (re.compile(r'\bSIC\b',                   re.IGNORECASE), 'SIC'),
]

def normalize_date(d: str) -> str:
    """
    Accepts:
      - '' or 'today'        → returns today in YYYY-MM-DD
      - 'YYYY-MM-DD'         → returns as-is
      - 'DD-MM-YYYY'         → converts to YYYY-MM-DD
    """
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

def classify(name: str) -> str:
    txt = name or ''
    for pat, label in CLASS_PATTERNS:
        if pat.search(txt):
            return label
    return 'Other'

def enrich_sic(codes: list[str]) -> tuple[str,str,str]:
    joined, descs, uses = ",".join(codes), [], []
    for code in codes:
        if code in SIC_LOOKUP:
            d, u = SIC_LOOKUP[code]
            descs.append(d)
            uses.append(u)
    return joined, "; ".join(descs), "; ".join(uses)

def fetch_companies_on(ds: str, api_key: str) -> list[dict]:
    records = []
    start_index = 0

    while True:
        enforce_rate_limit()
        params = {
            'incorporated_from': ds,
            'incorporated_to':   ds,
            'size':              FETCH_SIZE,
            'start_index':       start_index
        }
        try:
            resp = requests.get(CH_API_URL, auth=(api_key, ''), params=params, timeout=10)
            resp.raise_for_status()
            record_call()
            data = resp.json()
            items = data.get('items', [])
        except Exception as e:
            log.warning(f"{ds} @index {start_index}: {e}")
            time.sleep(RETRY_DELAY)
            continue

        now = datetime.now(timezone.utc)
        for c in items:
            nm    = c.get('title') or c.get('company_name') or ''
            num   = c.get('company_number','')
            codes = c.get('sic_codes', [])
            sic_codes, sic_desc, sic_use = enrich_sic(codes)
            records.append({
                'Company Name':       nm,
                'Company Number':     num,
                'Incorporation Date': c.get('date_of_creation',''),
                'Status':             c.get('company_status',''),
                'Source':             c.get('source',''),
                'Date Downloaded':    now.strftime('%Y-%m-%d'),
                'Time Discovered':    now.strftime('%H:%M:%S'),
                'Category':           classify(nm),
                'SIC Codes':          sic_codes,
                'SIC Description':    sic_desc,
                'Typical Use Case':   sic_use
            })

        if len(items) < FETCH_SIZE:
            break
        start_index += FETCH_SIZE

    return records

def run_for_date_range(start_date: str, end_date: str):
    sd = datetime.strptime(start_date, '%Y-%m-%d')
    ed = datetime.strptime(end_date,   '%Y-%m-%d')
    if sd > ed:
        log.error("start_date cannot be after end_date")
        sys.exit(1)

    new_records = []
    cur = sd
    while cur <= ed:
        ds = cur.strftime('%Y-%m-%d')
        log.info(f"Fetching companies for {ds}")
        new_records += fetch_companies_on(ds, API_KEY)
        cur += timedelta(days=1)

    # Load or init master
    if os.path.exists(MASTER_CSV):
        try:
            df_master = pd.read_csv(MASTER_CSV)
        except pd.errors.EmptyDataError:
            df_master = pd.DataFrame(columns=FIELDS)
    else:
        df_master = pd.DataFrame(columns=FIELDS)

    # Append & dedupe
    if new_records:
        df_new = pd.DataFrame(new_records, columns=FIELDS)
        df_all = pd.concat([df_master, df_new], ignore_index=True)
        df_all.drop_duplicates(subset=['Company Number'], keep='first', inplace=True)
    else:
        df_all = df_master

    # Sort & enforce field order
    df_all.sort_values('Incorporation Date', ascending=False, inplace=True)
    df_all = df_all[FIELDS]

    # Write master outputs
    df_all.to_csv(MASTER_CSV, index=False)
    df_all.to_excel(MASTER_XLSX, index=False, engine='openpyxl')  
    log.info(f"Wrote master CSV/XLSX ({len(df_all)} rows)")

    # ─── Build relevant = filtered subset of master ──────────────
    mask_cat = df_all['Category'] != 'Other'
    mask_sic = df_all['SIC Description'].astype(bool)
    df_rel   = df_all[mask_cat | mask_sic]

    # Write relevant outputs
    df_rel.to_csv(RELEVANT_CSV, index=False)
    df_rel.to_excel(RELEVANT_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote relevant CSV/XLSX ({len(df_rel)} rows)")

def main():
    global API_KEY
    p = argparse.ArgumentParser()
    p.add_argument('--start_date', default='', help='YYYY-MM-DD or "today"')
    p.add_argument('--end_date',   default='', help='YYYY-MM-DD or "today"')
    args = p.parse_args()

    API_KEY = os.getenv('CH_API_KEY')
    if not API_KEY:
        log.error('CH_API_KEY not set')
        sys.exit(1)

    sd = normalize_date(args.start_date)
    ed = normalize_date(args.end_date)
    log.info(f"Starting run {sd} → {ed}")
    run_for_date_range(sd, ed)

if __name__ == '__main__':
    main()
