#!/usr/bin/env python3
"""
fund_tracker.py

- Fetches Companies House data by date
- Honors shared buffered rate limit (550 calls per 5 min)
- Writes master & relevant CSV/XLSX to docs/assets/data/
- Filters relevant to:
    • any Category != "Other" (all your keywords)
    OR
    • any SIC code in your 16-code lookup
- Enriches SIC codes with description & typical use case
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

# ─── Data columns & classification patterns ───────────────────────────────────
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
        nm = c.get('title','') or c.get('company_name','')
        sc, sd, su = enrich_sic(c.get('sic_codes', []))
        rows.append({
            'Company Name':       nm,
            'Company Number':     c.get('company_number',''),
            'Incorporation Date': c.get('date_of_creation',''),
            'Status':             c.get('company_status',''),
            'Source':             c.get('source',''),
            'Date Downloaded':    now.strftime('%Y-%m-%d'),
            'Time Discovered':    now.strftime('%H:%M:%S'),
            'Category':           classify(nm),
            'SIC Codes':          sc,
            'SIC Description':    sd,
            'Typical Use Case':   su
        })
    return rows

def fetch_companies_on(ds, api_key):
    records, now = [], datetime.now(timezone.utc)

    # first page
    enforce_rate_limit()
    resp = requests.get(API_URL, auth=(api_key,''), params={
        'incorporated_from': ds, 'incorporated_to': ds,
        'size': FETCH_SIZE, 'start_index': 0
    }, timeout=10)
    resp.raise_for_status(); record_call()
    data = resp.json()
    records.extend(_parse_items(data.get('items', []), now))

    # remaining pages
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

def run_for_range(sd: str, ed: str):
    API_KEY = os.getenv('CH_API_KEY')
    if not API_KEY:
        log.error("CH_API_KEY is not set—please configure it in your CI secrets or env")
        sys.exit(1)

    log.info(f"Starting fund_tracker.py; {get_remaining_calls()} calls remaining")
    os.makedirs(DATA_DIR, exist_ok=True)

    # parse dates
    try:
        sd_dt = datetime.strptime(sd, '%Y-%m-%d')
        ed_dt = datetime.strptime(ed, '%Y-%m-%d')
    except Exception as e:
        log.error(f"Bad date inputs: {e}")
        sys.exit(1)
    if sd_dt > ed_dt:
        log.error("start_date > end_date")
        sys.exit(1)

    # collect all records
    all_recs, cur = [], sd_dt
    while cur <= ed_dt:
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
    log.info(f"Wrote master ({len(df_all)} rows)")

    # ─── Relevant CSV/XLSX ────────────────────────────────────────────────────
    # include any non-Other category OR any strict lookup SIC code
    mask_cat = df_all['Category'] != 'Other'
    mask_sic = df_all['SIC Codes'].str.split(',').apply(
        lambda codes: any(code in SIC_LOOKUP for code in codes)
    )
    df_rel = df_all[mask_cat | mask_sic]

    df_rel.to_csv(RELEVANT_CSV, index=False)
    df_rel.to_excel(RELEVANT_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote relevant ({len(df_rel)} rows)")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', default='today', help='YYYY-MM-DD or today')
    parser.add_argument('--end_date',   default='today', help='YYYY-MM-DD or today')
    args = parser.parse_args()

    sd = normalize_date(args.start_date)
    ed = normalize_date(args.end_date)
    run_for_range(sd, ed)
