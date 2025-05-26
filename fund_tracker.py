#!/usr/bin/env python3
"""
fund_tracker.py (revised)

- Fetches Companies House data by date
- Honors buffered rate limit (550 calls per 5 min)
- Builds `df_all` in memory, writes master CSV/XLSX
- Filters `df_rel` directly from `df_all`
- Asserts `relevant` ⊆ `master` in-memory
- Writes relevant CSV/XLSX
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

# ─── SIC Lookup ────────────────────────────────────────────────────────────────
SIC_LOOKUP = {
    '64205': ("Activities of financial services holding companies",
              "Holding-company SPV for portfolio-company equity stakes, co-investment vehicles, master/feeder hubs."),
    # … all your other codes …
    '70221': ("Financial management (of companies and enterprises)",
              "Treasury, capital-raising and internal financial services arm."),
}

# ─── Classification ─────────────────────────────────────────────────────────────
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
    (re.compile(r'\bAdvisors?\b',             re.IGNORECASE), 'Advisors'),
    (re.compile(r'\bPartners\b',              re.IGNORECASE), 'Partners'),
]

# ─── Logger Setup ──────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
log = get_logger('fund_tracker', LOG_FILE)

def normalize_date(d: str) -> str:
    if not d or d.lower()=='today':
        return date.today().strftime('%Y-%m-%d')
    for fmt in ('%Y-%m-%d','%d-%m-%Y'):
        try:
            return datetime.strptime(d, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    log.error(f"Invalid date: {d}")
    sys.exit(1)

def classify(name: str) -> str:
    for pat, lbl in CLASS_PATTERNS:
        if pat.search(name or ''):
            return lbl
    return 'Other'

def enrich_sic(codes):
    joined = ",".join(codes or [])
    descs, uses = [], []
    for c in (codes or []):
        if c in SIC_LOOKUP:
            d,u = SIC_LOOKUP[c]
            descs.append(d); uses.append(u)
    return joined, "; ".join(descs), "; ".join(uses)

def fetch_items_for_date(ds, api_key):
    records, now = [], datetime.now(timezone.utc)
    def fetch_page(start):
        enforce_rate_limit()
        r = requests.get(API_URL, auth=(api_key,''), params={
            'incorporated_from': ds, 'incorporated_to': ds,
            'size': FETCH_SIZE, 'start_index': start
        }, timeout=10)
        r.raise_for_status(); record_call()
        return r.json()

    # first page + total
    data0 = fetch_page(0)
    total = data0.get('total_results',0)
    pages = (total + FETCH_SIZE -1)//FETCH_SIZE

    for items in [data0.get('items',[])] + [
        fetch_page(i*FETCH_SIZE).get('items',[]) for i in range(1,pages)
    ]:
        for c in items:
            nm = c.get('title','') or c.get('company_name','')
            sc, sd, su = enrich_sic(c.get('sic_codes', []))
            records.append({
                'Company Name': nm,
                'Company Number': c.get('company_number',''),
                'Incorporation Date': c.get('date_of_creation',''),
                'Status': c.get('company_status',''),
                'Source': c.get('source',''),
                'Date Downloaded': now.strftime('%Y-%m-%d'),
                'Time Discovered': now.strftime('%H:%M:%S'),
                'Category': classify(nm),
                'SIC Codes': sc,
                'SIC Description': sd,
                'Typical Use Case': su
            })
    return records

def run_for_range(sd, ed):
    key = os.getenv('CH_API_KEY') or sys.exit(log.error("CH_API_KEY unset"))
    log.info(f"Starting fund_tracker; {get_remaining_calls()} calls remain")
    os.makedirs(DATA_DIR, exist_ok=True)

    # collect
    start, all_recs = datetime.fromisoformat(sd), []
    end = datetime.fromisoformat(ed)
    while start <= end:
        ds = start.strftime('%Y-%m-%d')
        log.info(f"Fetching {ds}")
        all_recs.extend(fetch_items_for_date(ds, key))
        start += timedelta(days=1)

    # build df_all
    df_master = pd.DataFrame(all_recs, columns=FIELDS) \
        if all_recs else pd.DataFrame(columns=FIELDS)
    # append existing if any
    if os.path.exists(MASTER_CSV):
        try:
            old = pd.read_csv(MASTER_CSV)
            df_master = pd.concat([old, df_master], ignore_index=True) \
                .drop_duplicates('Company Number', keep='first')
        except pd.errors.EmptyDataError:
            pass

    df_master.sort_values('Incorporation Date', ascending=False, inplace=True)
    df_master.to_csv(MASTER_CSV, index=False)
    df_master.to_excel(MASTER_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote master ({len(df_master)})")

    # filter df_all → df_rel
    mask_cat = df_master['Category'] != 'Other'
    mask_sic = df_master['SIC Codes'].str.split(',').apply(
        lambda codes: any(c in SIC_LOOKUP for c in codes)
    )
    df_rel = df_master[mask_cat | mask_sic]

    # assert by construction
    assert set(df_rel['Company Number']).issubset(df_master['Company Number'])

    df_rel.to_csv(RELEVANT_CSV, index=False)
    df_rel.to_excel(RELEVANT_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote relevant ({len(df_rel)})")

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--start_date',default='today')
    p.add_argument('--end_date', default='today')
    a=p.parse_args()
    run_for_range(normalize_date(a.start_date), normalize_date(a.end_date))
