#!/usr/bin/env python3
import os
import json
import pandas as pd
import requests
from datetime import datetime
from rate_limiter import enforce_rate_limit, record_call
from logger import log  # ← shared logger

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
API_BASE        = 'https://api.company-information.service.gov.uk/company'
CH_KEY          = os.getenv('CH_API_KEY')
RELEVANT_CSV    = 'assets/data/relevant_companies.csv'
DIRECTORS_JSON  = 'assets/data/directors.json'
LOG_FILE        = 'assets/logs/fund_tracker.log'  # all logs go here

def load_relevant_numbers():
    df = pd.read_csv(RELEVANT_CSV, dtype=str)
    return set(df['Company Number'].dropna())

def load_existing_map():
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON, 'r') as f:
            return json.load(f)
    return {}

def fetch_one(number):
    enforce_rate_limit()
    url    = f"{API_BASE}/{number}/officers"
    params = {'register_view': 'true'}
    try:
        resp = requests.get(url, auth=(CH_KEY, ''), params=params, timeout=10)
        resp.raise_for_status()
        record_call()
        items = resp.json().get('items', [])
    except Exception as e:
        log.warning(f"{number}: fetch error: {e}")
        return None

    ROLES = {'director', 'member'}
    active = [o for o in items if o.get('officer_role') in ROLES and o.get('resigned_on') is None]
    chosen = active or [o for o in items if o.get('officer_role') in ROLES]

    directors = []
    for off in chosen:
        dob = off.get('date_of_birth') or {}
        y, m = dob.get('year'), dob.get('month')
        dob_str = f"{y}-{int(m):02d}" if y and m else str(y) if y else ''
        directors.append({
            'title':            off.get('name'),
            'appointment':      off.get('snippet') or '',
            'dateOfBirth':      dob_str,
            'appointmentCount': off.get('appointment_count'),
            'selfLink':         off['links'].get('self'),
            'officerRole':      off.get('officer_role'),
            'nationality':      off.get('nationality'),
            'occupation':       off.get('occupation'),
        })
    return directors

def main():
    log.info("Starting director fetch run")
    if not CH_KEY:
        log.error("CH_API_KEY missing")
        return

    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
    relevant = load_relevant_numbers()
    existing = load_existing_map()
    pending  = [num for num in relevant if num not in existing]

    if not pending:
        log.info("No new companies to fetch directors for.")
        with open(DIRECTORS_JSON, 'w') as f:
            json.dump(existing, f, separators=(',',':'))
        return

    log.info(f"{len(pending)} pending companies to fetch")
    for num in pending:
        dirs = f
