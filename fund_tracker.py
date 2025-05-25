#!/usr/bin/env python3
"""
fund_tracker.py

- Fetches Companies House data by incorporation date
- Classifies via regex and SIC lookup
- Writes directly to docs/assets/data/{master,relevant}_{companies}.csv/.xlsx
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import re
import requests
import pandas as pd
from rate_limiter import enforce_rate_limit, record_call

# ─── Configuration ─────────────────────────────────────────────────────────────
API_URL        = 'https://api.company-information.service.gov.uk/advanced-search/companies'
DATA_DIR       = 'docs/assets/data'
MASTER_CSV     = f'{DATA_DIR}/master_companies.csv'
MASTER_XLSX    = f'{DATA_DIR}/master_companies.xlsx'
RELEVANT_CSV   = f'{DATA_DIR}/relevant_companies.csv'
RELEVANT_XLSX  = f'{DATA_DIR}/relevant_companies.xlsx'
LOG_FILE       = 'fund_tracker.log'
FETCH_SIZE     = 100
SIC_LOOKUP     = {
    '64205': ("Activities of financial services holding companies",
              "Holding-company SPV for portfolio-company equity stakes, co-investment vehicles, master/feeder hubs."),
    '70221': ("Financial management (of companies and enterprises)",
              "Treasury, capital-raising and internal financial services arm."),
}
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
    (re.compile(r'\bSIC\b',                   re.IGNORECASE), 'SIC'),
]

# ─── Logging Setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)
os.makedirs(DATA_DIR, exist_ok=True)

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

def enrich_sic(codes: list[str]) -> tuple[str,str,str]:
    joined, descs, uses = ",".join(codes), [], []
    for code in codes:
        if code in SIC_LOOKUP:
            d,u = SIC_LOOKUP[code]
            descs.append(d); uses.append(u)
    return joined, "; ".join(descs), "; ".join(uses)

def fetch_companies_on(ds: str, api_key: str) -> list[dict]:
    records, start_index = [], 0
    while True:
        enforce_rate_limit()
        resp = requests.get(API_URL, auth=(api_key,''), params={
            'incorporated_from': ds,
            'incorporated_to':   ds,
            'size': FETCH_SIZE,
            'start_index': start_index
        }, timeout=10)
        try:
            resp.raise_for_status()
            record_call()
        except:
            log.warning(f"{ds}@{start_index} error, retry in 5s")
            time.sleep(5); continue

        data = resp.json().get('items', [])
        now = datetime.now(timezone.utc)
        for c in data:
            nm = c.get('title') or c.get('company_name') or ''
            codes = c.get('sic_codes', [])
            sc, sd, su = enrich_sic(codes)
            records.append({
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
        if len(data) < FETCH_SIZE: break
        start_index += FETCH_SIZE
    return records

def run_for_range(sd: str, ed: str):
    sd_dt = datetime.strptime(sd,'%Y-%m-%d')
    ed_dt = datetime.strptime(ed,'%Y-%m-%d')
    if sd_dt>ed_dt: log.error("start_date > end_date"); sys.exit(1)

    new_records=[]
    cur=sd_dt
    while cur<=ed_dt:
        ds=cur.strftime('%Y-%m-%d')
        log.info(f"Fetching {ds}")
        new_records += fetch_companies_on(ds, API_KEY)
        cur += timedelta(days=1)

    # load or init master
    if os.path.exists(MASTER_CSV):
        try: df_master = pd.read_csv(MASTER_CSV)
        except pd.errors.EmptyDataError: df_master = pd.DataFrame(columns=FIELDS)
    else:
        df_master = pd.DataFrame(columns=FIELDS)

    if new_records:
        df_new = pd.DataFrame(new_records,columns=FIELDS)
        df_all = pd.concat([df_master,df_new],ignore_index=True)
        df_all.drop_duplicates('Company Number',keep='first',inplace=True)
    else:
        df_all = df_master

    df_all.sort_values('Incorporation Date',ascending=False,inplace=True)
    df_all = df_all[FIELDS]
    df_all.to_csv(MASTER_CSV,index=False)
    df_all.to_excel(MASTER_XLSX,index=False,engine='openpyxl')
    log.info(f"Wrote master ({len(df_all)} rows)")

    mask_cat = df_all['Category']!='Other'
    mask_sic = df_all['SIC Description'].astype(bool)
    df_rel = df_all[mask_cat|mask_sic]
    df_rel.to_csv(RELEVANT_CSV,index=False)
    df_rel.to_excel(RELEVANT_XLSX,index=False,engine='openpyxl')
    log.info(f"Wrote relevant ({len(df_rel)} rows)")

if __name__=='__main__':
    API_KEY = os.getenv('CH_API_KEY') or sys.exit(log.error('CH_API_KEY unset'))
    p=argparse.ArgumentParser()
    p.add_argument('--start_date',default='',help='YYYY-MM-DD or today')
    p.add_argument('--end_date',  default='',help='YYYY-MM-DD or today')
    args=p.parse_args()
    sd=normalize_date(args.start_date)
    ed=normalize_date(args.end_date)
    log.info(f"Run {sd} → {ed}")
    run_for_range(sd,ed)
