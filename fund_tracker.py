#!/usr/bin/env python3
"""
fund_tracker.py

- Always regenerates master & relevant CSV/XLSX if missing
- Merges new data into existing files when present
- Safe pagination via total_results
- Honors the shared buffered rate limit (550 calls per 5 minutes)
- Logs to assets/logs/fund_tracker.log
"""

import argparse
import os
import sys
import re
import requests
import pandas as pd
from datetime import date, datetime, timedelta, timezone

from rate_limiter import enforce_rate_limit, record_call, get_remaining_calls
from logger import get_logger

# ─── Paths & Config ────────────────────────────────────────────────────────────
API_URL       = 'https://api.company-information.service.gov.uk/advanced-search/companies'
DATA_DIR      = 'docs/assets/data'
LOG_DIR       = 'assets/logs'
LOG_FILE      = os.path.join(LOG_DIR, 'fund_tracker.log')
MASTER_CSV    = os.path.join(DATA_DIR, 'master_companies.csv')
MASTER_XLSX   = os.path.join(DATA_DIR, 'master_companies.xlsx')
RELEVANT_CSV  = os.path.join(DATA_DIR, 'relevant_companies.csv')
RELEVANT_XLSX = os.path.join(DATA_DIR, 'relevant_companies.xlsx')
FETCH_SIZE    = 100

# ─── SIC Lookup (code → (description, use case)) ──────────────────────────────
SIC_LOOKUP = {
    '64205': ("Activities of financial services holding companies",
              "Holding‐company SPV for portfolio‐company equity stakes, …"),
    # … other codes as before …
    '70221': ("Financial management (of companies and enterprises)",
              "Treasury, capital‐raising and internal financial services arm."),
}

# ─── Columns & Classification Patterns ─────────────────────────────────────────
FIELDS = [
    'Company Name','Company Number','Incorporation Date',
    'Status','Source','Date Downloaded','Time Discovered',
    'Category','SIC Codes','SIC Description','Typical Use Case'
]
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
]

# ─── Only these go into “Fund Entities” tab ────────────────────────────────────
INCLUSION_CATEGORIES = {'LLP','LP','GP','Fund'}

# ─── Logger Setup ──────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
log = get_logger('fund_tracker', LOG_FILE)
log.info(f"Starting fund_tracker.py; {get_remaining_calls()} calls remaining")

def normalize_date(d: str) -> str:
    if not d or d.lower() == 'today':
        return date.today().strftime('%Y-%m-%d')
    for fmt in ('%Y-%m-%d','%d-%m-%Y'):
        try:
            return datetime.strptime(d, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    log.error(f"Invalid date format: {d}")
    sys.exit(1)

def classify(name: str) -> str:
    for pat, label in CLASS_PATTERNS:
        if pat.search(name or ''):
            return label
    return 'Other'

def enrich_sic(codes):
    joined = ",".join(codes or [])
    descs, uses = [], []
    for code in codes or []:
        if code in SIC_LOOKUP:
            d, u = SIC_LOOKUP[code]
            descs.append(d); uses.append(u)
    return joined, "; ".join(descs), "; ".join(uses)

def _parse_items(items, now):
    rows = []
    for c in items:
        name = c.get('title','') or c.get('company_name','')
        sc, sd, su = enrich_sic(c.get('sic_codes', []))
        rows.append({
            'Company Name':       name,
            'Company Number':     c.get('company_number',''),
            'Incorporation Date': c.get('date_of_creation',''),
            'Status':             c.get('company_status',''),
            'Source':             c.get('source',''),
            'Date Downloaded':    now.strftime('%Y-%m-%d'),
            'Time Discovered':    now.strftime('%H:%M:%S'),
            'Category':           classify(name),
            'SIC Codes':          sc,
            'SIC Description':    sd,
            'Typical Use Case':   su
        })
    return rows

def fetch_companies_on(ds, api_key):
    records, now = [], datetime.now(timezone.utc)

    # 1) Fetch first page to get total_results
    enforce_rate_limit()
    resp = requests.get(API_URL, auth=(api_key,''), params={
        'incorporated_from': ds, 'incorporated_to': ds,
        'size': FETCH_SIZE, 'start_index': 0
    }, timeout=10)
    resp.raise_for_status(); record_call()
    data = resp.json()
    records.extend(_parse_items(data.get('items', []), now))

    # 2) Calculate exact pages, loop safely
    total = data.get('total_results', 0)
    pages = (total + FETCH_SIZE - 1) // FETCH_SIZE
    for p in range(1, pages):
        enforce_rate_limit()
        resp = requests.get(API_URL, auth=(api_key,''), params={
            'incorporated_from': ds, 'incorporated_to': ds,
            'size': FETCH_SIZE, 'start_index': p*FETCH_SIZE
        }, timeout=10)
        resp.raise_for_status(); record_call()
        items = resp.json().get('items', [])
        if not items:
            break
        records.extend(_parse_items(items, now))

    return records

def run_for_range(start_date: str, end_date: str):
    # Ensure data directory exists up front
    os.makedirs(DATA_DIR, exist_ok=True)

    # Normalize and validate dates
    sd = datetime.strptime(start_date, '%Y-%m-%d')
    ed = datetime.strptime(end_date,   '%Y-%m-%d')
    if sd > ed:
        log.error("start_date > end_date"); sys.exit(1)

    all_recs = []
    cur = sd
    while cur <= ed:
        ds = cur.strftime('%Y-%m-%d')
        log.info(f"Fetching companies for {ds}")
        all_recs.extend(fetch_companies_on(ds, API_KEY))
        cur += timedelta(days=1)

    # ─── Master CSV/XLSX ───────────────────────────────────────────────────────
    if os.path.exists(MASTER_CSV):
        try:
            df_master = pd.read_csv(MASTER_CSV)
        except pd.errors.EmptyDataError:
            df_master = pd.DataFrame(columns=FIELDS)
    else:
        df_master = pd.DataFrame(columns=FIELDS)

    if all_recs:
        df_new = pd.DataFrame(all_recs, columns=FIELDS)
        df_all = pd.concat([df_master, df_new], ignore_index=True)
        df_all.drop_duplicates('Company Number', keep='first', inplace=True)
    else:
        df_all = df_master

    df_all.sort_values('Incorporation Date', ascending=False, inplace=True)
    df_all = df_all[FIELDS]
    df_all.to_csv(MASTER_CSV, index=False)
    df_all.to_excel(MASTER_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote master ({len(df_all)} rows) to CSV & XLSX")

    # ─── Relevant CSV/XLSX ────────────────────────────────────────────────────
    mask_cat = df_all['Category'].isin(INCLUSION_CATEGORIES)
    mask_sic = df_all['SIC Description'].astype(bool)
    df_rel   = df_all[mask_cat | mask_sic]
    df_rel.to_csv(RELEVANT_CSV, index=False)
    df_rel.to_excel(RELEVANT_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote relevant ({len(df_rel)} rows) to CSV & XLSX")

if __name__ == '__main__':
    API_KEY = os.getenv('CH_API_KEY') or sys.exit(log.error('CH_API_KEY unset'))
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', default='today', help='YYYY-MM-DD or today')
    parser.add_argument('--end_date',   default='today', help='YYYY-MM-DD or today')
    args = parser.parse_args()

    sd = normalize_date(args.start_date)
    ed = normalize_date(args.end_date)
    run_for_range(sd, ed)
