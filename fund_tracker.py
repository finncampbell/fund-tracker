#!/usr/bin/env python3
"""
fund_tracker.py

- Fetches Companies House data by incorporation date (default: today)
- Retries on transient errors
- Logs to fund_tracker.log
- Builds two outputs:
  • master_companies.csv / .xlsx  (full history, with Category)
  • relevant_companies.csv / .xlsx (only matching Categories)
Each file always includes the full header row in the same order.
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta

import requests
import pandas as pd

# CONFIGURATION
CH_API_URL    = 'https://api.company-information.service.gov.uk/advanced-search/companies'
MASTER_XLSX   = 'master_companies.xlsx'
MASTER_CSV    = 'assets/data/master_companies.csv'
RELEVANT_XLSX = 'relevant_companies.xlsx'
RELEVANT_CSV  = 'assets/data/relevant_companies.csv'
LOG_FILE      = 'fund_tracker.log'
RETRY_COUNT   = 3
RETRY_DELAY   = 5     # seconds
FETCH_SIZE    = 100   # items per request

# Column order for all CSV/XLSX outputs
FIELDS = [
  'Company Name',
  'Company Number',
  'Incorporation Date',
  'Status',
  'Source',
  'Date Downloaded',
  'Time Discovered',
  'Category'
]

# Keywords for classification, in priority order
KEYWORDS = [
  'Ventures','Capital','Equity',
  'Advisors','Partners','SIC',
  'Fund','GP','LP','LLP','Investments'
]

# Set up logging
logging.basicConfig(
  filename=LOG_FILE,
  level=logging.INFO,
  format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.WARNING)
log.addHandler(console)

def normalize_date(d: str) -> str:
  """Empty or 'today' → today’s date; else return as-is."""
  if not d or d.lower() == 'today':
    return date.today().strftime('%Y-%m-%d')
  return d

def classify(name: str) -> str:
  """Return first matching keyword or 'Other'."""
  low = (name or '').lower()
  for kw in KEYWORDS:
    if kw.lower() in low:
      return kw
  return 'Other'

def fetch_companies_on(date_str: str, api_key: str) -> list[dict]:
  """Hit the advanced-search endpoint, retry on failure."""
  auth = (api_key, '')
  params = {
    'incorporated_from': date_str,
    'incorporated_to':   date_str,
    'size':              FETCH_SIZE
  }
  for attempt in range(1, RETRY_COUNT + 1):
    try:
      resp = requests.get(CH_API_URL, auth=auth, params=params, timeout=10)
      if resp.status_code == 200:
        items = resp.json().get('items', [])
        now = datetime.utcnow()
        recs = []
        for c in items:
          name = c.get('title') or c.get('company_name') or ''
          recs.append({
            'Company Name':       name,
            'Company Number':     c.get('company_number',''),
            'Incorporation Date': c.get('date_of_creation',''),
            'Status':             c.get('company_status',''),
            'Source':             c.get('source',''),
            'Date Downloaded':    now.strftime('%Y-%m-%d'),
            'Time Discovered':    now.strftime('%H:%M:%S'),
            'Category':           classify(name)
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
  """Fetch each day, append & dedupe master, then write master + relevant files."""
  sd = datetime.strptime(start_date, '%Y-%m-%d')
  ed = datetime.strptime(end_date, '%Y-%m-%d')
  if sd > ed:
    log.error("start_date cannot be after end_date")
    sys.exit(1)

  new_records = []
  cur = sd
  while cur <= ed:
    ds = cur.strftime('%Y-%m-%d')
    log.info(f'Fetching companies for {ds}')
    new_records.extend(fetch_companies_on(ds, API_KEY))
    cur += timedelta(days=1)

  os.makedirs(os.path.dirname(MASTER_CSV), exist_ok=True)

  # Load or init master DataFrame
  if os.path.exists(MASTER_CSV):
