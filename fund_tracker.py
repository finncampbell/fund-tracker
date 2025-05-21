#!/usr/bin/env python3
"""
fund_tracker.py

- Fetches Companies House data by incorporation date (default: today)
- Retries on transient errors
- Logs to fund_tracker.log (now at DEBUG level)
- Builds two outputs:
  • master_companies.csv / .xlsx  (full history, with Category, SIC Codes, Descriptions)
  • relevant_companies.csv / .xlsx (only matching Categories or target SIC codes)
Each file always includes the full header row in the same order.
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
RETRY_DELAY   = 5     # seconds
FETCH_SIZE    = 100   # items per request

# -- Logging setup (now at DEBUG) --
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)

# Your target SIC lookup table
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
              "Treasury, capital-raising and internal financial services arm.")
}

# Column order for outputs
FIELDS = [
    'Company Name','Company Number','Incorporation Date',
    'Status','Source','Date Downloaded','Time Discovered',
    'Category','SIC Codes','SIC Description','Typical Use Case'
]

# -- Ordered regex patterns for classification --
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
    (re.compile(r'\bSIC\b',                   re.IGNORECASE), 'SIC'),
]

def normalize_date(d: str) -> str:
    if not d or d.lower() == 'today':
        return date.today().strftime('%Y-%m-%d')
    return d

def classify(name: str) -> str:
    """
    Return the first matching label from CLASS_PATTERNS,
    falling back to 'Other' if nothing matches.
    """
    txt = name or ''
    for pat, label in CLASS_PATTERNS:
        if pat.search(txt):
            return label
    return 'Other'

def enrich_sic(codes: list[str]) -> tuple[str,str,str]:
    joined = ",".join(codes)
    descs, uses = [], []
    for code in codes:
        if code in SIC_LOOKUP:
            d,u = SIC_LOOKUP[code]
            descs.append(d); uses.append(u)
    return joined, "; ".join(descs), "; ".join(uses)

def fetch_companies_on(date_str: str, api_key: str) -> list[dict]:
    auth = (api_key, '')
    params = {
        'incorporated_from': date_str,
        'incorporated_to':   date_str,
        'size':              FETCH_SIZE
    }
    for attempt in range(1, RETRY_COUNT+1):
        try:
            resp = requests.get(CH_API_URL, auth=auth, params=params, timeout=10)
            if resp.status_code == 200:
                items = resp.json().get('items', [])
                now = datetime.utcnow()
                recs = []
                for c in items:
                    nm    = c.get('title') or c.get('company_name') or ''
                    codes = c.get('sic_codes', [])
                    sic_codes, sic_desc, sic_use = enrich_sic(codes)

                    # -- Debug classification here --
                    category = classify(nm)
                    log.debug(f"DEBUG classify: “{nm}” → {category}")

                    recs.append({
                        'Company Name':       nm,
                        'Company Number':     c.get('company_number',''),
                        'Incorporation Date': c.get('date_of_creation',''),
                        'Status':             c.get('company_status',''),
                        'Source':             c.get('source',''),
                        'Date Downloaded':    now.strftime('%Y-%m-%d'),
                        'Time Discovered':    now.strftime('%H:%M:%S'),
                        'Category':           category,
                        'SIC Codes':          sic_codes,
                        'SIC Description':    sic_desc,
                        'Typical Use Case':   sic_use
                    })
                return recs
            else:
                log.warning(f'Non-200 ({resp.status_code}) on {date_str}, attempt {attempt}')
        except Exception as e:
            log.warning(f'Error on {date_str}, attempt {attempt}: {e}')
        time.sleep(RETRY_DELAY)
    log.error(f'Failed to fetch for {date_str}')
    return []

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
        log.info(f'Fetching companies for {ds}')
        new_records += fetch_companies_on(ds, API_KEY)
        cur += timedelta(days=1)

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

def main():
    global API_KEY
    p = argparse.ArgumentParser()
    p.add_argument('--start_date', default='', help='YYYY-MM-DD or "today"')
    p.add_argument('--end_date',   default='', help='YYYY-MM-DD or "today"')
    args = p.parse_args()

    API_KEY = os.getenv('CH_API_KEY')
    if not API_KEY:
        log.error('CH_API_KEY not set')
        sys.exit(1)

    sd = normalize_date(args.start_date)
    ed = normalize_date(args.end_date)
    log.info(f'Starting run {sd} → {ed}')
    run_for_date_range(sd, ed)

if __name__ == '__main__':
    main()
