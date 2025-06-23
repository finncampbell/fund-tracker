#!/usr/bin/env python3
"""
scripts/fetch_persons.py

Fetch main individual records (core metadata) for all IRNs discovered or a limited subset.
Updates data/fca_persons.json by merging new entries with existing ones.
"""
import os
import json
import argparse
import requests
from rate_limiter import RateLimiter

# ─── Paths & Config ──────────────────────────────────────────────────────────
SCRIPT_DIR    = os.path.dirname(__file__)
DATA_DIR      = os.path.abspath(os.path.join(SCRIPT_DIR, '../data'))
IND_BY_FIRM   = os.path.join(DATA_DIR, 'fca_individuals_by_firm.json')
PERSONS_JSON  = os.path.join(DATA_DIR, 'fca_persons.json')

# ─── FCA Register API setup ────────────────────────────────────────────────────
API_EMAIL = os.getenv('FCA_API_EMAIL')
API_KEY   = os.getenv('FCA_API_KEY')
if not API_EMAIL or not API_KEY:
    raise EnvironmentError('FCA_API_EMAIL and FCA_API_KEY must be set in the environment')

BASE_URL = 'https://register.fca.org.uk/services/V0.1'
HEADERS  = {
    'Accept':       'application/json',
    'X-AUTH-EMAIL': API_EMAIL,
    'X-AUTH-KEY':   API_KEY,
}

limiter = RateLimiter()

def fetch_json(url: str) -> dict:
    limiter.wait()
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()

def fetch_individual_record(irn: str) -> dict | None:
    """Fetch core individual record for a single IRN and return selected fields."""
    try:
        pkg = fetch_json(f"{BASE_URL}/Individuals/{irn}")
    except Exception as e:
        print(f"⚠️  Failed fetch for IRN {irn}: {e}")
        return None

    data_list = pkg.get('Data') or []
    if not data_list:
        print(f"⚠️  No Data block for IRN {irn}")
        return None

    record = data_list[0]
    # Unwrap if wrapped in 'Details'
    info = record.get('Details', record)

    # Select core fields, with fallbacks for name
    selected = {
        'irn': irn,
        'name': info.get('Name') or info.get('Full Name') or info.get('Commonly Used Name'),
        'status': info.get('Status'),
        'date_of_birth': info.get('Date of Birth'),
        'system_timestamp': info.get('System Timestamp')
    }
    return selected

def main():
    parser = argparse.ArgumentParser(description='Fetch and merge individual records')
    parser.add_argument('--limit', type=int, help='Only process first N IRNs for testing')
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)

    # Load IRNs from fca_individuals_by_firm.json
    if not os.path.exists(IND_BY_FIRM):
        print(f"❌ Missing {IND_BY_FIRM}: run fetch_firm_individuals.py first")
        return
    with open(IND_BY_FIRM, 'r', encoding='utf-8') as f:
        firm_map = json.load(f)
    # Collect unique IRNs
    all_irns = []
    for frn, entries in firm_map.items():
        for e in entries:
            irn_val = e.get('IRN')
            if irn_val:
                all_irns.append(irn_val)
    # Dedupe preserving order
    seen = set()
    irns = [x for x in all_irns if not (x in seen or seen.add(x))]

    if args.limit:
        irns = irns[: args.limit]
        print(f"🔍 Test mode: will fetch {len(irns)} individual records")

    # Load existing store
    if os.path.exists(PERSONS_JSON):
        with open(PERSONS_JSON, 'r', encoding='utf-8') as f:
            data = json.load(f)
        store = data if isinstance(data, dict) else {}
    else:
        store = {}

    # Fetch & merge
    for irn in irns:
        rec = fetch_individual_record(irn)
        if rec:
            store[irn] = rec
            print(f"✅ Fetched and stored individual record for IRN {irn}")

    # Write back
    with open(PERSONS_JSON, 'w', encoding='utf-8') as f:
        json.dump(store, f, indent=2, ensure_ascii=False)
    print(f"✅ Wrote {len(store)} individual records to {PERSONS_JSON}")

if __name__ == '__main__':
    main()
