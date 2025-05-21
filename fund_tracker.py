#!/usr/bin/env python3
"""
fund_tracker.py
...
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
import re
import requests
import pandas as pd

# CONFIGURATION
CH_API_URL    = 'https://api.company-information.service.gov.uk/advanced-search/companies'
MASTER_XLSX   = 'assets/data/master_companies.xlsx'
MASTER_CSV    = 'assets/data/master_companies.csv'
RELEVANT_XLSX = 'assets/data/relevant_companies.xlsx'
RELEVANT_CSV  = 'assets/data/relevant_companies.csv'
LOG_FILE      = 'fund_tracker.log'
RETRY_COUNT   = 3
RETRY_DELAY   = 5
FETCH_SIZE    = 100

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)

# … SIC_LOOKUP, FIELDS, CLASS_PATTERNS, normalize_date(), classify(), enrich_sic(), fetch_companies_on() unchanged …

def run_for_date_range(start_date: str, end_date: str):
    # … fetch new_records as before …

    os.makedirs(os.path.dirname(MASTER_CSV), exist_ok=True)

    # Load or init master
    if os.path.exists(MASTER_CSV):
        try:
            df_master = pd.read_csv(MASTER_CSV)
        except pd.errors.EmptyDataError:
            df_master = pd.DataFrame(columns=FIELDS)
    else:
        df_master = pd.DataFrame(columns=FIELDS)

    # Append & dedupe
    if new_records:
        df_new = pd.DataFrame(new_records, columns=FIELDS)
        df_all = pd.concat([df_master, df_new], ignore_index=True)
        df_all.drop_duplicates(subset=['Company Number'], keep='first', inplace=True)
    else:
        df_all = df_master

    # ───── NEW LINE: re-classify *all* rows ──────────────────────────
    df_all['Category'] = df_all['Company Name'].apply(classify)
    # ────────────────────────────────────────────────────────────────

    # Sort & enforce columns
    df_all.sort_values('Incorporation Date', ascending=False, inplace=True)
    df_all = df_all[FIELDS]

    # Write master
    df_all.to_excel(MASTER_XLSX, index=False)
    df_all.to_csv(MASTER_CSV, index=False)
    log.info(f'Master updated: {len(df_all)} rows')

    # Build relevant
    mask_cat = df_all['Category'] != 'Other'
    mask_sic = df_all['SIC Description'].astype(bool)
    df_rel   = df_all[mask_cat | mask_sic]

    # Write relevant
    df_rel.to_excel(RELEVANT_XLSX, index=False)
    df_rel.to_csv(RELEVANT_CSV, index=False)
    log.info(f'Relevant updated: {len(df_rel)} rows')

# … main() unchanged …
