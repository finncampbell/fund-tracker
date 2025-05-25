#!/usr/bin/env python3
import os
import json
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
from rate_limiter import enforce_rate_limit, record_call
from logger import log  # ← shared logger

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
API_BASE        = 'https://api.company-information.service.gov.uk/company'
CH_KEY          = os.getenv('CH_API_KEY')
RELEVANT_CSV    = 'assets/data/relevant_companies.csv'
DIRECTORS_JSON  = 'assets/data/directors.json'

def load_relevant():
    df = pd.read_csv(RELEVANT_CSV, dtype=str, parse_dates=['Incorporation Date'])
    return df

def load_existing():
    if os.path.exists(DIRECTORS_JSON):
        with open(DIRECTORS_JSON, 'r') as f:
            return json.load(f)
    return {}

def fetch_officers(company_number):
    enforce_rate_limit()
    url = f"{API_BASE}/{company_number}/officers"
    params = {'register_view': 'true'}
    try:
        resp = requests.get(url, auth=(CH_KEY, ''), params=params, timeout=10)
        resp.raise_for_status()
        record_call()
        return resp.json().get('items', [])
    except Exception as e:
        log.warning(f"{company_number}: fetch error: {e}")
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', required=True, help='YYYY-MM-DD')
    parser.add_argument('--end_date',   required=True, help='YYYY-MM-DD')
    args = parser.parse_args()

    log.info(f"Starting historical backfill {args.start_date} → {args.end_date}")
    if not CH_KEY:
        log.error("CH_API_KEY missing")
        return

    sd = datetime.fromisoformat(args.start_date).date()
    ed = datetime.fromisoformat(args.end_date).date()

    df = load_relevant()
    existing = load_existing()

    mask_pending = ~df['Company Number'].isin(existing.keys())
    mask_hist = df['Incorporation Date'].dt.date.between(sd, ed)
    df_pending = df[mask_pending & mask_hist]

    if df_pending.empty:
        log.info("No historical companies to backfill.")
        os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
        with open(DIRECTORS_JSON, 'w') as f:
            json.dump(existing, f, separators=(',',':'))
        return

    df_pending.sort_values('Incorporation Date', ascending=False, inplace=True)
    pending = df_pending['Company Number'].tolist()
    log.info(f"{len(pending)} companies to backfill directors for")

    now = datetime.now()
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    end_time = next_hour - timedelta(minutes=5)
    log.info(f"Backfill will stop at {end_time.time()}")

    for num in pending:
        if datetime.now() >= end_time:
            log.info("Reached cutoff time; stopping backfill loop")
            break

        items = fetch_officers(num)
        if items is None:
            continue

        ROLES = {'director', 'member'}
        actives = [o for o in items if o.get('officer_role') in ROLES and o.get('resigned_on') is None]
        chosen = actives or [o for o in items if o.get('officer_role') in ROLES]

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

        existing[num] = directors
        log.info(f"Backfilled {len(directors)} officers for {num}")

    os.makedirs(os.path.dirname(DIRECTORS_JSON), exist_ok=True)
    with open(DIRECTORS_JSON, 'w') as f:
        json.dump(existing, f, separators=(',',':'))
    log.info(f"Wrote directors.json with {len(existing)} companies")

if __name__ == '__main__':
    main()
