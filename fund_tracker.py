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

# ─── Master/Relevant column schema ───────────────────────────────────────────────

SCHEMA_COLUMNS = [
    'Company Name', 'Company Number', 'Incorporation Date', 'Status', 'Source',
    'Date Downloaded', 'Time Discovered', 'Category', 'SIC Codes',
    'SIC Description', 'Typical Use Case'
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
    """Return first matching category or 'Other'."""
    for pat, label in CLASS_PATTERNS:
        if pat.search(name or ''):
            return label
    return 'Other'

# ─── SIC Lookup & Enrichment ────────────────────────────────────────────────────

SIC_LOOKUP = {
    # ... (as before) ...
}

def enrich_sic(codes):
    joined = ",".join(codes or [])
    descs, uses = [], []
    for c in (codes or []):
        if c in SIC_LOOKUP:
            d, u = SIC_LOOKUP[c]
            descs.append(d)
            uses.append(u)
    return joined, "; ".join(descs), "; ".join(uses)

def has_target_sic(cell: str) -> bool:
    if not cell or not isinstance(cell, str):
        return False
    return any(code in cell.split(",") for code in SIC_LOOKUP)

# ─── Date Normalization ─────────────────────────────────────────────────────────

def normalize_date(d: str) -> str:
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

    load_rate_limit_state(RATE_STATE_FILE)
    log.info(f"Starting fund_tracker run {sd} → {ed}")

    # load/run-level failures
    if os.path.exists(FAILED_PAGES_FILE):
        with open(FAILED_PAGES_FILE) as f:
            prev = json.load(f)
        failed_pages = { (r['date'], r['offset']): r['count'] for r in prev }
    else:
        failed_pages = {}
    new_failed = {}
    all_records = []

    cur = datetime.fromisoformat(sd)
    end = datetime.fromisoformat(ed)
    while cur <= end:
        ds = cur.strftime('%Y-%m-%d')
        log.info(f"Fetching companies for {ds}")

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
        now = datetime.now(timezone.utc)

        # process page 0
        for c in first.get('items', []):
            name = c.get('title') or c.get('company_name') or ''
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
            offset = p * FETCH_SIZE
            key = (ds, offset)
            try:
                batch = fetch_page(ds, offset, api_key)
                for c in batch.get('items', []):
                    name = c.get('title') or c.get('company_name') or ''
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
            except Exception:
                cnt = failed_pages.get(key, 0) + 1
                if cnt < MAX_RUN_RETRIES:
                    new_failed[key] = cnt
                else:
                    log.error(f"Dead page {key} after {cnt} runs")

        cur += timedelta(days=1)

    # retry old failures once
    for (ds, offset), cnt in failed_pages.items():
        if cnt >= MAX_RUN_RETRIES:
            continue
        key = (ds, offset)
        try:
            batch = fetch_page(ds, offset, api_key)
            now = datetime.now(timezone.utc)
            for c in batch.get('items', []):
                name = c.get('title') or c.get('company_name') or ''
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
        except Exception:
            new_cnt = cnt + 1
            if new_cnt < MAX_RUN_RETRIES:
                new_failed[key] = new_cnt
            else:
                log.error(f"Dead on retry: {key}")

    # persist state
    save_rate_limit_state(RATE_STATE_FILE)
    out = [{'date': d, 'offset': o, 'count': c} for ((d, o), c) in new_failed.items()]
    with open(FAILED_PAGES_FILE, 'w') as f:
        json.dump(out, f, indent=2)

    # build master DataFrame
    if os.path.exists(MASTER_CSV):
        try:
            df_master = pd.read_csv(MASTER_CSV)
        except pd.errors.EmptyDataError:
            df_master = pd.DataFrame()
    else:
        df_master = pd.DataFrame()

    if all_records:
        df_new = pd.DataFrame(all_records)
        df_master = pd.concat([df_master, df_new], ignore_index=True) \
                       .drop_duplicates('Company Number', keep='first')

    # ensure master headers even if empty
    if df_master.empty:
        df_master = pd.DataFrame(columns=SCHEMA_COLUMNS)
    else:
        df_master = df_master.reindex(columns=SCHEMA_COLUMNS)

    if 'Incorporation Date' in df_master.columns:
        df_master.sort_values('Incorporation Date', ascending=False, inplace=True)

    df_master.to_csv(MASTER_CSV, index=False)
    df_master.to_excel(MASTER_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote master ({len(df_master)} rows)")

    # filter relevant
    mask_cat = df_master['Category'] != 'Other'
    mask_sic = df_master['SIC Codes'].apply(has_target_sic)
    df_rel = df_master[mask_cat | mask_sic]

    # ensure relevant headers
    if df_rel.empty:
        df_rel = pd.DataFrame(columns=SCHEMA_COLUMNS)
    else:
        df_rel = df_rel.reindex(columns=SCHEMA_COLUMNS)

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
