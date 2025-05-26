#!/usr/bin/env python3
"""
fund_tracker.py

- Fetches Companies House data by date
- Handles pagination via total_results
- Honors the shared buffered rate limit (550 calls per 5 minutes)
- Writes master & relevant CSV/XLSX into docs/assets/data/
- Logs to assets/logs/fund_tracker.log
"""

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
import re
import requests
import pandas as pd

from rate_limiter import enforce_rate_limit, record_call, get_remaining_calls
from logger import get_logger

# ─── Config ────────────────────────────────────────────────────────────────────
API_URL      = 'https://api.company-information.service.gov.uk/advanced-search/companies'
DATA_DIR     = 'docs/assets/data'
LOG_DIR      = 'assets/logs'
LOG_FILE     = os.path.join(LOG_DIR, 'fund_tracker.log')
MASTER_CSV   = f'{DATA_DIR}/master_companies.csv'
MASTER_XLSX  = f'{DATA_DIR}/master_companies.xlsx'
RELEVANT_CSV = f'{DATA_DIR}/relevant_companies.csv'
RELEVANT_XLSX= f'{DATA_DIR}/relevant_companies.xlsx'
FETCH_SIZE   = 100

# ─── SIC lookup ────────────────────────────────────────────────────────────────
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

# ─── Columns & classification regexes ─────────────────────────────────────────
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

# ─── Only LLP/LP/GP/Fund for Fund Entities tab ─────────────────────────────────
INCLUSION_CATEGORIES = {'LLP','LP','GP','Fund'}

# ─── Logger ─────────────────────────────────────────────────────────────────────
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
    log.error(f"Invalid date format: {d}"); sys.exit(1)

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
    out = []
    for c in items:
        nm = c.get('title','') or c.get('company_name','')
        sc, sd, su = enrich_sic(c.get('sic_codes', []))
        out.append({
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
    return out

def fetch_companies_on(ds, api_key):
    records, now = [], datetime.now(timezone.utc)

    # First page: get total_results
    enforce_rate_limit()
    resp = requests.get(API_URL, auth=(api_key,''), params={
        'incorporated_from': ds,
        'incorporated_to':   ds,
        'size': FETCH_SIZE,
        'start_index': 0
    }, timeout=10)
    resp.raise_for_status(); record_call()

    data = resp.json()
    records.extend(_parse_items(data.get('items', []), now))
    total_results = data.get('total_results', 0)
    max_pages = (total_results + FETCH_SIZE - 1) // FETCH_SIZE

    for page in range(1, max_pages):
        start_index = page * FETCH_SIZE
        enforce_rate_limit()
        resp = requests.get(API_URL, auth=(api_key,''), params={
            'incorporated_from': ds,
            'incorporated_to':   ds,
            'size': FETCH_SIZE,
            'start_index': start_index
        }, timeout=10)
        resp.raise_for_status(); record_call()

        items = resp.json().get('items', [])
        if not items: break
        records.extend(_parse_items(items, now))

    return records

def run_for_range(sd, ed):
    sd_dt = datetime.strptime(sd, '%Y-%m-%d')
    ed_dt = datetime.strptime(ed, '%Y-%m-%d')
    if sd_dt > ed_dt:
        log.error("start_date > end_date"); sys.exit(1)

    all_recs, cur = [], sd_dt
    while cur <= ed_dt:
        ds = cur.strftime('%Y-${m}-%d')
        log.info(f"Fetching companies for {ds}")
        all_recs.extend(fetch_companies_on(ds, API_KEY))
        cur += timedelta(days=1)

    # Write master & relevant as before (unchanged)
    …
