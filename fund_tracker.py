#!/usr/bin/env python3
"""
fund_tracker.py

- Fetches Companies House data by incorporation date (default: today)
- Retries on errors, logs to fund_tracker.log
- Writes both CSV and XLSX (explicitly via openpyxl), and logs each write
"""

import argparse, logging, os, sys, time
from datetime import date, datetime, timedelta
import re, requests, pandas as pd

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

# Ensure output directory exists (for both CSV and XLSX)
os.makedirs(os.path.dirname(MASTER_CSV), exist_ok=True)

# ─── SIC Lookup & Fields ───────────────────────────────────────────────────────
SIC_LOOKUP = {
    # … your SIC_LOOKUP entries …
}
FIELDS = [
    'Company Name','Company Number','Incorporation Date',
    'Status','Source','Date Downloaded','Time Discovered',
    'Category','SIC Codes','SIC Description','Typical Use Case'
]

# ─── Classification Patterns ───────────────────────────────────────────────────
CLASS_PATTERNS = [
    (re.compile(r'\bL[\.\-\s]?L[\.\-\s]?P\b', re.I), 'LLP'),
    (re.compile(r'\bL[\.\-\s]?P\b',           re.I), 'LP'),
    (re.compile(r'\bG[\.\-\s]?P\b',           re.I), 'GP'),
    (re.compile(r'\bFund\b',                  re.I), 'Fund'),
    (re.compile(r'\bVentures?\b',             re.I), 'Ventures'),
    (re.compile(r'\bInvestment(s)?\b',        re.I), 'Investments'),
    (re.compile(r'\bCapital\b',               re.I), 'Capital'),
    (re.compile(r'\bEquity\b',                re.I), 'Equity'),
    (re.compile(r'\bAdvisors\b',              re.I), 'Advisors'),
    (re.compile(r'\bPartners\b',              re.I), 'Partners'),
    (re.compile(r'\bSIC\b',                   re.I), 'SIC'),
]

def normalize_date(d: str) -> str:
    if not d or d.lower()=='today':
        return date.today().strftime('%Y-%m-%d')
    return d

def classify(name: str) -> str:
    txt = name or ''
    for pat,label in CLASS_PATTERNS:
        if pat.search(txt):
            return label
    return 'Other'

def enrich_sic(codes: list[str]) -> tuple[str,str,str]:
    joined, descs, uses = ",".join(codes), [], []
    for code in codes:
        if code in SIC_LOOKUP:
            d,u = SIC_LOOKUP[code]
            descs.append(d); uses.append(u)
    return joined, "; ".join(descs), "; ".join(uses)

def fetch_companies_on(ds: str, api_key: str) -> list[dict]:
    auth = (api_key,'')
    params = {'incorporated_from': ds, 'incorporated_to': ds, 'size': FETCH_SIZE}
    for attempt in range(1, RETRY_COUNT+1):
        try:
            r = requests.get(CH_API_URL, auth=auth, params=params, timeout=10)
            if r.status_code==200:
                items = r.json().get('items', [])
                now = datetime.utcnow()
                recs = []
                for c in items:
                    nm = c.get('title') or c.get('company_name') or ''
                    codes = c.get('sic_codes', [])
                    sic_codes, sic_desc, sic_use = enrich_sic(codes)
                    cat = classify(nm)
                    recs.append({
                        'Company Name':       nm,
                        'Company Number':     c.get('company_number',''),
                        'Incorporation Date': c.get('date_of_creation',''),
                        'Status':             c.get('company_status',''),
                        'Source':             c.get('source',''),
                        'Date Downloaded':    now.strftime('%Y-%m-%d'),
                        'Time Discovered':    now.strftime('%H:%M:%S'),
                        'Category':           cat,
                        'SIC Codes':          sic_codes,
                        'SIC Description':    sic_desc,
                        'Typical Use Case':   sic_use
                    })
                return recs
            else:
                log.warning(f"Non-200 {r.status_code} on {ds}")
        except Exception as e:
            log.warning(f"Error on {ds}: {e}")
        time.sleep(RETRY_DELAY)
    log.error(f"Failed fetch for {ds}")
    return []

def run_for_date_range(sd: str, ed: str):
    start, end = datetime.strptime(sd,'%Y-%m-%d'), datetime.strptime(ed,'%Y-%m-%d')
    if start > end:
        log.error("start_date > end_date"); sys.exit(1)

    all_new = []
    cur = start
    while cur <= end:
        ds = cur.strftime('%Y-%m-%d')
        log.info(f"Fetching {ds}")
        all_new += fetch_companies_on(ds, API_KEY)
        cur += timedelta(days=1)

    # Load existing master
    if os.path.exists(MASTER_CSV):
        try:
            df_master = pd.read_csv(MASTER_CSV)
        except pd.errors.EmptyDataError:
            df_master = pd.DataFrame(columns=FIELDS)
    else:
        df_master = pd.DataFrame(columns=FIELDS)

    # Append new & dedupe
    if all_new:
        df_new = pd.DataFrame(all_new, columns=FIELDS)
        df = pd.concat([df_master, df_new], ignore_index=True)
        df.drop_duplicates('Company Number',keep='first',inplace=True)
    else:
        df = df_master

    # Re-classify all rows (safety)
    df['Category'] = df['Company Name'].apply(classify)

    # Sort & enforce columns
    df.sort_values('Incorporation Date', ascending=False, inplace=True)
    df = df[FIELDS]

    # Write master CSV & XLSX
    df.to_csv(MASTER_CSV, index=False)
    df.to_excel(MASTER_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote master CSV: {MASTER_CSV}")
    log.info(f"Wrote master XLSX: {MASTER_XLSX}")

    # Build & write relevant outputs
    mask = (df['Category']!='Other') | df['SIC Description'].astype(bool)
    df_rel = df[mask]
    df_rel.to_csv(RELEVANT_CSV, index=False)
    df_rel.to_excel(RELEVANT_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote relevant CSV: {RELEVANT_CSV}")
    log.info(f"Wrote relevant XLSX: {RELEVANT_XLSX}")

def main():
    global API_KEY
    p = argparse.ArgumentParser()
    p.add_argument('--start_date', default='', help='YYYY-MM-DD or today')
    p.add_argument('--end_date',   default='', help='YYYY-MM-DD or today')
    args = p.parse_args()

    API_KEY = os.getenv('CH_API_KEY')
    if not API_KEY:
        log.error("CH_API_KEY missing"); sys.exit(1)

    sd = normalize_date(args.start_date)
    ed = normalize_date(args.end_date)
    log.info(f"Run {sd} → {ed}")
    run_for_date_range(sd, ed)

if __name__=='__main__':
    main()
