#!/usr/bin/env python3
"""
fund_tracker.py

Fetch Companies House data by date range, enrich with SIC lookups and category,
and write master & relevant CSV/XLSX into docs/assets/data/.

Features:
- Shared JSON-backed rate limiter with 600 calls/5min minus 50-call buffer
- Incremental updates: appends new data, dedupes by Company Number
- In-memory subset guarantee for relevant slice
- Centralized logging to assets/logs/fund_tracker.log
"""

import os
import sys
import re
import json
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

# Ensure directories exist
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
    '64209': ("Activities of other holding companies n.e.c.",
              "Catch-all SPV: protected cells, cell companies, bespoke feeder vehicles."),
    '64301': ("Activities of investment trusts",
              "Closed-ended listed investment trusts (e.g. LSE-quoted funds)."),
    '64302': ("Activities of unit trusts",
              "On-shore unit trusts (including feeder trusts)."),
    '64303': ("Activities of venture and development capital companies",
              "Venture Capital Trusts (VCTs) and similar “development” schemes."),
    '64304': ("Activities of open-ended investment companies",
              "OEICs (master-fund and sub-fund layers of umbrella structures)."),
    '64305': ("Activities of property unit trusts",
              "Property-unit-trust vehicles (including REIT feeder trusts)."),
    '64306': ("Activities of real estate investment trusts",
              "UK-regulated REIT companies."),
    '64921': ("Credit granting by non-deposit-taking finance houses",
              "Direct-lending SPVs (senior debt, unitranche loans)."),
    '64922': ("Activities of mortgage finance companies",
              "Mortgage-debt vehicles (commercial/mortgage-backed SPVs)."),
    '64929': ("Other credit granting n.e.c.",
              "Mezzanine/sub-ordinated debt or hybrid capital vehicles."),
    '64991': ("Security dealing on own account",
              "Structured-credit/CLO collateral-management SPVs."),
    '64999': ("Financial intermediation not elsewhere classified",
              "Catch-all credit-oriented SPVs for novel lending structures."),
    '66300': ("Fund management activities",
              "AIFM or portfolio-management company itself."),
    '70100': ("Activities of head offices",
              "Group HQ: compliance, risk, finance, central strategy."),
    '70221': ("Financial management (of companies and enterprises)",
              "Treasury, capital-raising and internal financial services arm."),
}

# ─── Utility Functions ────────────────────────────────────────────────────────────
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
    enforce_rate_limit(); r = requests.get(
        API_URL, auth=(api_key,''), params={
            'incorporated_from': ds,
            'incorporated_to':   ds,
            'size': FETCH_SIZE,
            'start_index': start
        }, timeout=10)
    r.raise_for_status(); record_call()
    return r.json()

def fetch_companies_on(ds: str, api_key: str):
    records, start = [], 0
    # first page gives total_results
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

# ─── Main Processing ─────────────────────────────────────────────────────────────
def run_for_range(sd: str, ed: str):
    api_key = os.getenv('CH_API_KEY')
    if not api_key:
        log.error("CH_API_KEY unset!"); sys.exit(1)

    # Load & prune shared rate-limit state
    try:
        load_rate_limit_state(RATE_STATE)
    except Exception:
        log.debug("No existing rate-limit state; starting fresh")
    log.info("Starting fund_tracker run")

    # Collect new records
    all_recs = []
    start_dt = datetime.fromisoformat(sd)
    end_dt   = datetime.fromisoformat(ed)
    while start_dt <= end_dt:
        ds = start_dt.strftime('%Y-%m-%d')
        log.info(f"Fetching companies for {ds}")
        all_recs.extend(fetch_companies_on(ds, api_key))
        start_dt += timedelta(days=1)

    # Persist rate-limit state
    save_rate_limit_state(RATE_STATE)

    # Load or initialize master DataFrame
    if os.path.exists(MASTER_CSV):
        try:
            df_master = pd.read_csv(MASTER_CSV)
        except pd.errors.EmptyDataError:
            df_master = pd.DataFrame()
    else:
        df_master = pd.DataFrame()

    # Append new and dedupe
    if all_recs:
        df_new    = pd.DataFrame(all_recs)
        df_master = pd.concat([df_master, df_new], ignore_index=True) \
                      .drop_duplicates('Company Number', keep='first')

    # Sort & write master
    df_master.sort_values('Incorporation Date', ascending=False, inplace=True)
    df_master.to_csv(MASTER_CSV, index=False)
    df_master.to_excel(MASTER_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote master ({len(df_master)} rows)")

    # Build relevant slice in memory
    mask_cat = df_master['Category'] != 'Other'
    mask_sic = df_master['SIC Codes'].str.split(',') \
                   .apply(lambda codes: any(c in SIC_LOOKUP for c in codes))
    df_rel = df_master[mask_cat | mask_sic]
    # By construction, df_rel ⊆ df_master

    df_rel.to_csv(RELEVANT_CSV, index=False)
    df_rel.to_excel(RELEVANT_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote relevant ({len(df_rel)} rows)")

# ─── CLI Entrypoint ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description="Fund Tracker: ingest & enrich companies")
    p.add_argument('--start_date', default='today',
                   help='YYYY-MM-DD or "today"')
    p.add_argument('--end_date',   default='today',
                   help='YYYY-MM-DD or "today"')
    args = p.parse_args()
    sd = normalize_date(args.start_date)
    ed = normalize_date(args.end_date)
    run_for_range(sd, ed)
