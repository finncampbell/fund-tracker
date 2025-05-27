#!/usr/bin/env python3
"""
fund_tracker.py

Fetches newly incorporated UK companies from Companies House,
classifies and enriches them, and writes two data slices into docs/assets/data/:

  • master_companies.csv/.xlsx   — every company ever ingested (deduped)
  • relevant_companies.csv/.xlsx — only those with Category ≠ "Other" or matching our target SIC codes

It shares rate-limit state across runs, retries transient 5xx errors (3× with backoff),
and maintains a manifest of pages retried across up to 5 scheduled runs.
"""

import os
import sys
import re
import json
import time
import math
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

# ─── Configuration ───────────────────────────────────────────────────────────────

API_URL             = 'https://api.company-information.service.gov.uk/advanced-search/companies'
DATA_DIR            = 'docs/assets/data'
LOG_DIR             = 'assets/logs'
RATE_STATE_FILE     = os.path.join(LOG_DIR, 'rate_limit.json')
FAILED_PAGES_FILE   = os.path.join(DATA_DIR, 'failed_pages.json')

MASTER_CSV          = os.path.join(DATA_DIR, 'master_companies.csv')
MASTER_XLSX         = os.path.join(DATA_DIR, 'master_companies.xlsx')
RELEVANT_CSV        = os.path.join(DATA_DIR, 'relevant_companies.csv')
RELEVANT_XLSX       = os.path.join(DATA_DIR, 'relevant_companies.xlsx')

FETCH_SIZE          = 100

# HTTP retry config
INTERNAL_FETCH_RETRIES = 3   # per-call backoff retries
MAX_RUN_RETRIES       = 5   # how many scheduled runs to retry a page

# Ensure output directories exist
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
    (re.compile(r'\bAdvis(?:or|er)s?\b',      re.IGNORECASE), 'Advisors'),
    (re.compile(r'\bPartners\b',              re.IGNORECASE), 'Partners'),
]

def classify(name: str) -> str:
    """Return first matching category or 'Other'."""
    for pat, label in CLASS_PATTERNS:
        if pat.search(name or ''):
            return label
    return 'Other'

# ─── SIC Lookup & Enrichment ────────────────────────────────────────────────────

SIC_LOOKUP = {
    '64205': ("Activities of financial services holding companies",
              "Holding-company SPV for portfolio-company equity stakes, co-investment vehicles, master/feeder hubs."),
    '64209': ("Activities of other holding companies n.e.c.",
              "Catch-all SPV: protected cells, bespoke feeder vehicles."),
    '64301': ("Activities of investment trusts",
              "Closed-ended listed investment trusts (e.g. LSE-quoted funds)."),
    '64302': ("Activities of unit trusts",
              "On-shore unit trusts (including Feeder trusts)."),
    '64303': ("Activities of venture and development capital companies",
              "Venture Capital Trusts and similar schemes."),
    '64304': ("Activities of open-ended investment companies",
              "OEIC umbrella structures."),
    '64305': ("Activities of property unit trusts",
              "Property-unit-trust vehicles (REIT feeders)."),
    '64306': ("Activities of real estate investment trusts",
              "UK-regulated REIT companies."),
    '64921': ("Credit granting by non-deposit-taking finance houses",
              "Direct-lending SPVs."),
    '64922': ("Activities of mortgage finance companies",
              "Mortgage-backed SPVs."),
    '64929': ("Other credit granting n.e.c.",
              "Mezzanine or hybrid capital vehicles."),
    '64991': ("Security dealing on own account",
              "Structured-credit SPVs."),
    '64999': ("Financial intermediation not elsewhere classified",
              "Catch-all credit SPVs."),
    '66300': ("Fund management activities",
              "AIFM/portfolio-management firms."),
    '70100': ("Activities of head offices",
              "Group HQ: strategy, finance, compliance."),
    '70221': ("Financial management (of companies and enterprises)",
              "Internal treasury and finance arm."),
}

def enrich_sic(codes):
    """Return (joined_codes, descriptions, use_cases) for target SIC codes."""
    joined = ",".join(codes or [])
    descs, uses = [], []
    for c in (codes or []):
        if c in SIC_LOOKUP:
            d, u = SIC_LOOKUP[c]
            descs.append(d)
            uses.append(u)
    return joined, "; ".join(descs), "; ".join(uses)

def has_target_sic(cell: str) -> bool:
    """True if the 'SIC Codes' cell contains any of our target codes."""
    if not cell or not isinstance(cell, str):
        return False
    return any(code in cell.split(",") for code in SIC_LOOKUP)

# ─── Date Normalization ─────────────────────────────────────────────────────────

def normalize_date(d: str) -> str:
    """
    Accept 'today', 'YYYY-MM-DD' or 'DD-MM-YYYY'; return 'YYYY-MM-DD'.
    """
    if not d or d.lower() == 'today':
        return date.today().strftime('%Y-%m-%d')
    for fmt in ('%Y-%m-%d', '%d-%m-%Y'):
        try:
            return datetime.strptime(d, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    log.error(f"Invalid date format: {d}")
    sys.exit(1)

# ─── Fetch with Retry ────────────────────────────────────────────────────────────

def fetch_page(ds: str, offset: int, api_key: str) -> dict:
    """
    Fetch one page for date=ds, start_index=offset. Retries transient 5xx up to INTERNAL_FETCH_RETRIES.
    """
    params = {
        'incorporated_from': ds,
        'incorporated_to':   ds,
        'size': FETCH_SIZE,
        'start_index': offset
    }
    for attempt in range(1, INTERNAL_FETCH_RETRIES + 1):
        enforce_rate_limit()
        resp = requests.get(API_URL, auth=(api_key, ''), params=params, timeout=10)
        status = resp.status_code

        if 500 <= status < 600 and attempt < INTERNAL_FETCH_RETRIES:
            wait = 2 ** (attempt - 1)
            log.warning(f"Server error {status} on {ds}@{offset}; retry {attempt}/{INTERNAL_FETCH_RETRIES} after {wait}s")
            time.sleep(wait)
            continue

        try:
            resp.raise_for_status()
            record_call()
            return resp.json()
        except requests.HTTPError as e:
            log.error(f"HTTP error {status} on {API_URL} {params}: {e}")
            raise

    raise RuntimeError(f"Exhausted retries for {ds}@{offset}")

# ─── Main Run Logic ───────────────────────────────────────────────────────────────

def run_for_range(sd: str, ed: str):
    api_key = os.getenv('CH_API_KEY')
    if not api_key:
        log.error("CH_API_KEY is not set"); sys.exit(1)

    # Load shared rate-limit state
    load_rate_limit_state(RATE_STATE_FILE)
    log.info(f"Starting fund_tracker run {sd} → {ed}")

    # Load previous failures
    if os.path.exists(FAILED_PAGES_FILE):
        with open(FAILED_PAGES_FILE) as f:
            prev = json.load(f)
        failed_pages = { (r['date'], r['offset']): r['count'] for r in prev }
    else:
        failed_pages = {}
    new_failed = {}
    all_records = []

    # 1) Date-by-date pagination
    cur = datetime.fromisoformat(sd)
    end = datetime.fromisoformat(ed)
    while cur <= end:
        ds = cur.strftime('%Y-%m-%d')
        log.info(f"Fetching companies for {ds}")

        # First page
        try:
            first = fetch_page(ds, 0, api_key)
        except Exception:
            key = (ds, 0)
            cnt = failed_pages.get(key, 0) + 1
            if cnt < MAX_RUN_RETRIES:
                new_failed[key] = cnt
            else:
                log.error(f"Dead page {key} after {cnt} runs")
            cur += timedelta(days=1)
            continue

        total = first.get('total_results', 0) or 0
        pages = math.ceil(total / FETCH_SIZE)
        all_records.extend(first.get('items', []))

        # Subsequent pages
        for p in range(1, pages):
            offset = p * FETCH_SIZE
            key = (ds, offset)
            try:
                batch = fetch_page(ds, offset, api_key)
                all_records.extend(batch.get('items', []))
            except Exception:
                cnt = failed_pages.get(key, 0) + 1
                if cnt < MAX_RUN_RETRIES:
                    new_failed[key] = cnt
                else:
                    log.error(f"Dead page {key} after {cnt} runs")

        cur += timedelta(days=1)

    # 2) Retry old failures once more
    for (ds, offset), cnt in failed_pages.items():
        if cnt >= MAX_RUN_RETRIES:
            continue
        key = (ds, offset)
        try:
            batch = fetch_page(ds, offset, api_key)
            all_records.extend(batch.get('items', []))
        except Exception:
            new_cnt = cnt + 1
            if new_cnt < MAX_RUN_RETRIES:
                new_failed[key] = new_cnt
            else:
                log.error(f"Dead on retry: {key}")

    # 3) Persist rate-limit & failures manifest
    save_rate_limit_state(RATE_STATE_FILE)
    out = [{'date': d, 'offset': o, 'count': c} for ((d, o), c) in new_failed.items()]
    with open(FAILED_PAGES_FILE, 'w') as f:
        json.dump(out, f, indent=2)

    # 4) Load or initialize master DataFrame
    if os.path.exists(MASTER_CSV):
        try:
            df_master = pd.read_csv(MASTER_CSV)
        except pd.errors.EmptyDataError:
            df_master = pd.DataFrame()
    else:
        df_master = pd.DataFrame()

    # Append new, dedupe by Company Number
    if all_records:
        df_new = pd.DataFrame(all_records)
        df_master = pd.concat([df_master, df_new], ignore_index=True) \
                       .drop_duplicates('Company Number', keep='first')

    # Sort master if possible
    if 'Incorporation Date' in df_master.columns:
        df_master.sort_values('Incorporation Date', ascending=False, inplace=True)
    else:
        log.warning("No 'Incorporation Date' column found—skipping sort")

    # Write master slices
    df_master.to_csv(MASTER_CSV, index=False)
    df_master.to_excel(MASTER_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote master ({len(df_master)} rows)")

    # 5) Filter relevant
    if 'Category' in df_master.columns and 'SIC Codes' in df_master.columns:
        mask_cat = df_master['Category'] != 'Other'
        mask_sic = df_master['SIC Codes'].apply(has_target_sic)
        df_rel = df_master[mask_cat | mask_sic]
    else:
        df_rel = df_master.copy()
        log.warning("Missing 'Category' or 'SIC Codes'—using all as relevant")

    # 6) Ensure headers even if empty
    rel_columns = [
        'Company Name', 'Company Number', 'Incorporation Date', 'Status', 'Source',
        'Date Downloaded', 'Time Discovered', 'Category', 'SIC Codes',
        'SIC Description', 'Typical Use Case'
    ]
    if df_rel.empty:
        df_rel = pd.DataFrame(columns=rel_columns)
    else:
        # reindex to ensure correct column order
        df_rel = df_rel.reindex(columns=rel_columns)

    # Write relevant slices
    df_rel.to_csv(RELEVANT_CSV, index=False)
    df_rel.to_excel(RELEVANT_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote relevant ({len(df_rel)} rows)")

# ─── CLI Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Fund Tracker ingestion")
    parser.add_argument('--start_date', default='today',
                        help='YYYY-MM-DD, DD-MM-YYYY or "today"')
    parser.add_argument('--end_date',   default='today',
                        help='YYYY-MM-DD, DD-MM-YYYY or "today"')
    args = parser.parse_args()

    sd = normalize_date(args.start_date)
    ed = normalize_date(args.end_date)
    run_for_range(sd, ed)
