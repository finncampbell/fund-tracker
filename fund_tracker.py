#!/usr/bin/env python3
"""
fund_tracker.py

Fetch Companies House data by date range, enrich with SIC lookups and category,
and write master & relevant CSV/XLSX into docs/assets/data/.
"""

import os
import sys
import re
import logging
import time
import requests
import pandas as pd

from datetime import date, datetime, timedelta, timezone
from rate_limiter import enforce_rate_limit, record_call, load_rate_limit_state, save_rate_limit_state
from logger import log

# ─── Config & Paths ───────────────────────────────────────────────────────────────
API_URL       = 'https://api.company-information.service.gov.uk/advanced-search/companies'
DATA_DIR      = 'docs/assets/data'
LOG_DIR       = 'assets/logs'
RATE_STATE    = os.path.join(LOG_DIR, 'rate_limit.json')
MASTER_CSV    = os.path.join(DATA_DIR, 'master_companies.csv')
MASTER_XLSX   = os.path.join(DATA_DIR, 'master_companies.xlsx')
RELEVANT_CSV  = os.path.join(DATA_DIR, 'relevant_companies.csv')
RELEVANT_XLSX = os.path.join(DATA_DIR, 'relevant_companies.xlsx')
FETCH_SIZE    = 100

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# ─── Classification Patterns ─────────────────────────────────────────────────────
CLASS_PATTERNS = [
    (re.compile(r'\bL[\.\-\s]?L[\.\-\s]?P\b', re.IGNORECASE), 'LLP'),
    (re.compile(r'\bL[\.\-\s]?P\b',           re.IGNORECASE), 'LP'),
    (re.compile(r'\bG[\.\-\s]?P\b',           re.IGNORECASE), 'GP'),
    (re.compile(r'\bFund\b',                  re.IGNORECASE), 'Fund'),
    (re.compile(r'\bVentures?\b',             re.IGNORECASE), 'Ventures'),
    (re.compile(r'\bInvestment(s)?\b',        re.IGNORECASE), 'Investments'),
    (re.compile(r'\bCapital\b',               re.IGNORECASE), 'Capital'),
    (re.compile(r'\bEquity\b',                re.IGNORECASE), 'Equity'),
    (re.compile(r'\bAdvisors?\b',             re.IGNORECASE), 'Advisors'),
    (re.compile(r'\bPartners\b',              re.IGNORECASE), 'Partners'),
]
def classify(name: str) -> str:
    for pat, label in CLASS_PATTERNS:
        if pat.search(name or ''):
            return label
    return 'Other'

# ─── SIC Lookup ───────────────────────────────────────────────────────────────────
SIC_LOOKUP = {
    '64205': ("Activities of financial services holding companies",
              "Holding-company SPV for portfolio-company equity stakes, co-investment vehicles, master/feeder hubs."),
    # ... rest of your 16 codes ...
    '70221': ("Financial management (of companies and enterprises)",
              "Treasury, capital-raising and internal financial services arm."),
}

def normalize_date(d: str) -> str:
    if not d or d.lower() == 'today':
        return date.today().strftime('%Y-%m-%d')
    for fmt in ('%Y-%m-%d','%d-%m-%Y'):
        try:
            return datetime.strptime(d, fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass
    log.error(f"Invalid date format: {d}")
    sys.exit(1)

def enrich_sic(codes):
    joined = ",".join(codes or [])
    descs, uses = [], []
    for c in (codes or []):
        if c in SIC_LOOKUP:
            d,u = SIC_LOOKUP[c]
            descs.append(d); uses.append(u)
    return joined, "; ".join(descs), "; ".join(uses)

def fetch_page(ds: str, start: int, api_key: str):
    enforce_rate_limit()
    r = requests.get(API_URL, auth=(api_key,''), params={
            'incorporated_from': ds,
            'incorporated_to':   ds,
            'size': FETCH_SIZE,
            'start_index': start
        }, timeout=10)
    r.raise_for_status()
    record_call()
    return r.json()

def fetch_companies_on(ds: str, api_key: str):
    records, start = [], 0
    data0 = fetch_page(ds, 0, api_key)
    total = data0.get('total_results', 0)
    pages = (total + FETCH_SIZE - 1) // FETCH_SIZE
    now = datetime.now(timezone.utc)
    for page in range(pages):
        items = data0['items'] if page == 0 else fetch_page(ds, page*FETCH_SIZE, api_key)['items']
        for c in items:
            name = c.get('title','') or c.get('company_name','')
            sc, sd, su = enrich_sic(c.get('sic_codes', []))
            records.append({
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
    return records

def has_target_sic(cell):
    if not isinstance(cell, str) or not cell.strip():
        return False
    for code in cell.split(','):
        if code in SIC_LOOKUP:
            return True
    return False

def run_for_range(sd: str, ed: str):
    api_key = os.getenv('CH_API_KEY')
    if not api_key:
        log.error("CH_API_KEY unset!"); sys.exit(1)

    # Load & prune shared rate-limit state
    load_rate_limit_state(RATE_STATE)
    log.info(f"Starting fund_tracker run {sd}→{ed}")

    # Fetch
    all_recs = []
    cur = datetime.fromisoformat(sd)
    end = datetime.fromisoformat(ed)
    while cur <= end:
        ds = cur.strftime('%Y-%m-%d')
        log.info(f"Fetching companies for {ds}")
        all_recs += fetch_companies_on(ds, api_key)
        cur += timedelta(days=1)

    # Persist rate-limit state
    save_rate_limit_state(RATE_STATE)

    # Load or init master
    if os.path.exists(MASTER_CSV):
        try:
            df_master = pd.read_csv(MASTER_CSV)
        except pd.errors.EmptyDataError:
            df_master = pd.DataFrame()
    else:
        df_master = pd.DataFrame()

    # Append & dedupe
    if all_recs:
        df_new    = pd.DataFrame(all_recs)
        df_master = pd.concat([df_master, df_new], ignore_index=True) \
                      .drop_duplicates('Company Number', keep='first')

    # Sort & write master
    df_master.sort_values('Incorporation Date', ascending=False, inplace=True)
    df_master.to_csv(MASTER_CSV, index=False)
    df_master.to_excel(MASTER_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote master ({len(df_master)} rows)")

    # Build relevant slice
    mask_cat = df_master['Category'] != 'Other'
    mask_sic = df_master['SIC Codes'].apply(has_target_sic)
    df_rel   = df_master[mask_cat | mask_sic]

    df_rel.to_csv(RELEVANT_CSV, index=False)
    df_rel.to_excel(RELEVANT_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote relevant ({len(df_rel)} rows)")

if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--start_date', default='today')
    p.add_argument('--end_date',   default='today')
    args = p.parse_args()
    sd, ed = normalize_date(args.start_date), normalize_date(args.end_date)
    run_for_range(sd, ed)
