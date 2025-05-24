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
import math
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

# … your SIC_LOOKUP, FIELDS, CLASS_PATTERNS definitions …

def normalize_date(d: str) -> str:
    if not d or d.lower() == 'today':
        return date.today().strftime('%Y-%m-%d')
    try:
        return datetime.strptime(d, '%Y-%m-%d').strftime('%Y-%m-%d')
    except ValueError:
        pass
    try:
        return datetime.strptime(d, '%d-%m-%Y').strftime('%Y-%m-%d')
    except ValueError:
        log.error(f"Invalid date format: {d}")
        sys.exit(1)

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
            # retry once
            continue

        now = datetime.now(timezone.utc)
        for c in items:
            nm  = c.get('title') or c.get('company_name') or ''
            num = c.get('company_number','')
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

        # if fewer than FETCH_SIZE items, we’re done paging
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

    # … load/append/dedupe master, sort, write CSV/XLSX …

    # ─── Build relevant subset ───────────────────────────────────────
    mask_cat = df_all['Category'] != 'Other'
    mask_sic = df_all['SIC Description'].astype(bool)
    df_rel   = df_all[mask_cat | mask_sic]

    # … write relevant CSV/XLSX …
