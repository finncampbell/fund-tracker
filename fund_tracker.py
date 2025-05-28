#!/usr/bin/env python3
"""
fund_tracker.py

Fetches newly incorporated UK companies from Companies House,
classifies and enriches them (SIC lookup, category regex),
and writes two data slices into docs/assets/data/:

  • master_companies.csv/.xlsx   — every company ever ingested (deduped)
  • relevant_companies.csv/.xlsx — only those with a non-“Other” category
                                   OR matching one of our target SIC codes

Per‐call, 3× retries on 5xx, sharing a 550-call/5 min buffered rate limit.
"""

import os
import sys
import re
import time
import requests
import pandas as pd
from datetime import date, datetime, timedelta, timezone
import argparse

from rate_limiter import (
    enforce_rate_limit,
    record_call,
    load_rate_limit_state,
    save_rate_limit_state,
)
from logger import log

# ─── Paths & Configuration ───────────────────────────────────────────────────────
API_URL      = 'https://api.company-information.service.gov.uk/advanced-search/companies'
DATA_DIR     = 'docs/assets/data'
LOG_DIR      = 'assets/logs'
RATE_STATE   = os.path.join(LOG_DIR, 'rate_limit.json')

MASTER_CSV    = os.path.join(DATA_DIR, 'master_companies.csv')
MASTER_XLSX   = os.path.join(DATA_DIR, 'master_companies.xlsx')
RELEVANT_CSV  = os.path.join(DATA_DIR, 'relevant_companies.csv')
RELEVANT_XLSX = os.path.join(DATA_DIR, 'relevant_companies.xlsx')

FETCH_SIZE    = 100
RETRIES       = 3    # per-page 5xx retries

# Ensure directories exist
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# ─── Column schema ───────────────────────────────────────────────────────────────
SCHEMA_COLUMNS = [
    'Company Name','Company Number','Incorporation Date','Status','Source',
    'Date Downloaded','Time Discovered','Category','SIC Codes',
    'SIC Description','Typical Use Case'
]

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
    (re.compile(r'\bAdvis(?:or|er)s?\b',      re.IGNORECASE), 'Advisors'),
    (re.compile(r'\bPartners\b',              re.IGNORECASE), 'Partners'),
]
def classify(name: str) -> str:
    for pat, label in CLASS_PATTERNS:
        if pat.search(name or ''):
            return label
    return 'Other'

# ─── SIC Lookup & Enrichment ────────────────────────────────────────────────────
SIC_LOOKUP = {
    '64205': ("Activities of financial services holding companies",
              "Holding-company SPVs, co-investment vehicles, master/feeder hubs."),
    '64209': ("Activities of other holding companies n.e.c.",
              "Protected-cell SPVs, bespoke feeders."),
    '64301': ("Activities of investment trusts",
              "Closed-ended listed trusts."),
    '64302': ("Activities of unit trusts",
              "On-shore feeder trusts."),
    '64303': ("Activities of venture and development capital companies",
              "Venture Capital Trusts (VCTs)."),
    '64304': ("Activities of open-ended investment companies",
              "OEIC umbrella layers."),
    '64305': ("Activities of property unit trusts",
              "Property-unit-trust vehicles."),
    '64306': ("Activities of real estate investment trusts",
              "UK-regulated REITs."),
    '64921': ("Credit granting by non-deposit-taking finance houses",
              "Direct-lending SPVs."),
    '64922': ("Activities of mortgage finance companies",
              "Mortgage-backed SPVs."),
    '64929': ("Other credit granting n.e.c.",
              "Mezzanine/debt hybrid vehicles."),
    '64991': ("Security dealing on own account",
              "CLO collateral-management SPVs."),
    '64999': ("Financial intermediation not elsewhere classified",
              "Catch-all credit-oriented SPVs."),
    '66300': ("Fund management activities",
              "AIFMs and portfolio-management firms."),
    '70100': ("Activities of head offices",
              "Group HQ: strategy/finance/compliance."),
    '70221': ("Financial management (of companies and enterprises)",
              "Treasury and internal finance arm."),
}
def enrich_sic(codes):
    joined = ",".join(codes or [])
    descs, uses = [], []
    for c in (codes or []):
        if c in SIC_LOOKUP:
            d,u = SIC_LOOKUP[c]
            descs.append(d); uses.append(u)
    return joined, "; ".join(descs), "; ".join(uses)

def has_target_sic(cell: str) -> bool:
    if not cell or not isinstance(cell, str):
        return False
    return any(cd in SIC_LOOKUP for cd in cell.split(","))

# ─── Date Normalization ───────────────────────────────────────────────────────────
def normalize_date(d: str) -> str:
    if not d or d.lower() == 'today':
        return date.today().strftime('%Y-%m-%d')
    for fmt in ('%Y-%m-%d','%d-%m-%Y'):
        try:
            return datetime.strptime(d,fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    log.error(f"Invalid date format: {d}")
    sys.exit(1)

# ─── Fetch One Page with Retries (now swallows final 500) ────────────────────────
def fetch_page(ds: str, offset: int, api_key: str) -> dict:
    params = {
        'incorporated_from': ds,
        'incorporated_to':   ds,
        'size': FETCH_SIZE,
        'start_index': offset
    }
    for attempt in range(1, RETRIES+1):
        enforce_rate_limit()
        resp = requests.get(API_URL, auth=(api_key,''), params=params, timeout=10)

        # transient server error? retry with backoff
        if 500 <= resp.status_code < 600 and attempt < RETRIES:
            wait = 2 ** (attempt-1)
            log.warning(f"{ds}@{offset} 5xx ({resp.status_code}), retry {attempt}/{RETRIES} after {wait}s")
            time.sleep(wait)
            continue

        try:
            resp.raise_for_status()
            record_call()
            return resp.json()
        except requests.HTTPError as e:
            # final failure: log and return empty page
            log.error(f"{ds}@{offset} HTTP {resp.status_code}: {e}; skipping this page")
            return {'items': []}

    # should never reach here, but just in case
    log.error(f"{ds}@{offset} gave no valid response after {RETRIES} attempts; skipping")
    return {'items': []}

# ─── Main Run ───────────────────────────────────────────────────────────────────
def run_for_range(sd: str, ed: str):
    api_key = os.getenv('CH_API_KEY')
    if not api_key:
        log.error("CH_API_KEY unset!"); sys.exit(1)

    load_rate_limit_state(RATE_STATE)
    log.info(f"Starting fund_tracker run {sd} → {ed}")

    all_records = []
    cur = datetime.fromisoformat(sd)
    end = datetime.fromisoformat(ed)

    while cur <= end:
        ds = cur.strftime('%Y-%m-%d')
        log.info(f"Fetching companies for {ds}")

        # fetch page 0 and log raw JSON + total_results
        first = fetch_page(ds, 0, api_key)
        log.info(f"{ds}: raw first-page JSON: {first}")
        log.info(f"{ds}: total_results = {first.get('total_results')}")

        total = first.get('total_results', 0) or 0
        pages = (total + FETCH_SIZE - 1)//FETCH_SIZE
        now = datetime.now(timezone.utc)

        # page 0 items
        for c in first.get('items', []):
            name = c.get('title') or c.get('company_name','')
            sc, sd_desc, su = enrich_sic(c.get('sic_codes', []))
            all_records.append({
                'Company Name':       name,
                'Company Number':     c.get('company_number',''),
                'Incorporation Date': c.get('date_of_creation',''),
                'Status':             c.get('company_status',''),
                'Source':             c.get('source',''),
                'Date Downloaded':    now.strftime('%Y-%m-%d'),
                'Time Discovered':    now.strftime('%H:%M:%S'),
                'Category':           classify(name),
                'SIC Codes':          sc,
                'SIC Description':    sd_desc,
                'Typical Use Case':   su
            })

        # subsequent pages
        for p in range(1, pages):
            batch = fetch_page(ds, p*FETCH_SIZE, api_key)
            for c in batch.get('items', []):
                name = c.get('title') or c.get('company_name','')
                sc, sd_desc, su = enrich_sic(c.get('sic_codes', []))
                all_records.append({
                    'Company Name':       name,
                    'Company Number':     c.get('company_number',''),
                    'Incorporation Date': c.get('date_of_creation',''),
                    'Status':             c.get('company_status',''),
                    'Source':             c.get('source',''),
                    'Date Downloaded':    now.strftime('%Y-%m-%d'),
                    'Time Discovered':    now.strftime('%H:%M:%S'),
                    'Category':           classify(name),
                    'SIC Codes':          sc,
                    'SIC Description':    sd_desc,
                    'Typical Use Case':   su
                })

        cur += timedelta(days=1)

    save_rate_limit_state(RATE_STATE)

    # ─── Merge into master ────────────────────────────────────────────────────────
    if os.path.exists(MASTER_CSV):
        try:
            df_master = pd.read_csv(MASTER_CSV)
        except pd.errors.EmptyDataError:
            df_master = pd.DataFrame(columns=SCHEMA_COLUMNS)
    else:
        df_master = pd.DataFrame(columns=SCHEMA_COLUMNS)

    if all_records:
        df_new    = pd.DataFrame(all_records)
        df_master = pd.concat([df_master, df_new], ignore_index=True) \
                       .drop_duplicates('Company Number', keep='first')

    df_master.sort_values('Incorporation Date', ascending=False, inplace=True)
    df_master.to_csv(MASTER_CSV, index=False)
    df_master.to_excel(MASTER_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote master ({len(df_master)} rows)")

    # ─── Build relevant slice ────────────────────────────────────────────────────
    mask_cat = df_master['Category'] != 'Other'
    mask_sic = df_master['SIC Codes'].apply(has_target_sic)
    df_rel   = df_master[mask_cat | mask_sic]

    df_rel.to_csv(RELEVANT_CSV, index=False)
    df_rel.to_excel(RELEVANT_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote relevant ({len(df_rel)} rows)")

# ─── CLI Entrypoint ───────────────────────────────────────────────────────────────
if __name__ == '__main__':
    p = argparse.ArgumentParser(description="Fund Tracker ingestion")
    p.add_argument('--start_date', default='today',
                   help='YYYY-MM-DD, DD-MM-YYYY or "today"')
    p.add_argument('--end_date',   default='today',
                   help='YYYY-MM-DD, DD-MM-YYYY or "today"')
    args = p.parse_args()

    sd = normalize_date(args.start_date)
    ed = normalize_date(args.end_date)
    run_for_range(sd, ed)
