#!/usr/bin/env python3
"""
scripts/fetch_firm_individuals.py

Fetch the list of individuals for each firm (FRN) via the paginated /Firm/{frn}/Individuals endpoint.
Writes results for a single slice (offset + limit) to the given output file.
"""

import os
import json
import argparse
import requests
from rate_limiter import RateLimiter

# ─── Paths & Config ──────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(__file__)
DATA_DIR   = os.path.abspath(os.path.join(SCRIPT_DIR, '../data'))
FRNS_JSON  = os.path.join(DATA_DIR, 'all_frns_with_names.json')

# ─── FCA API setup ────────────────────────────────────────────────────────────
API_EMAIL = os.getenv('FCA_API_EMAIL')
API_KEY   = os.getenv('FCA_API_KEY')
if not API_EMAIL or not API_KEY:
    raise EnvironmentError('FCA_API_EMAIL and FCA_API_KEY must be set')

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

def fetch_paginated(url: str) -> list:
    items = []
    next_url = url
    while next_url:
        pkg = fetch_json(next_url)
        items.extend(pkg.get('Data') or [])
        next_url = pkg.get('ResultInfo',{}).get('Next')
    return items

def main():
    p = argparse.ArgumentParser(description='Fetch a slice of firm-individuals')
    p.add_argument('--offset', type=int, required=True, help='Zero-based start index')
    p.add_argument('--limit',  type=int, required=True, help='Number of FRNs to process')
    p.add_argument('--output', required=True, help='Path to write this chunk’s JSON')
    args = p.parse_args()

    # Load all FRNs
    frn_items = json.load(open(FRNS_JSON, encoding='utf-8'))
    frns = [item['frn'] for item in frn_items]

    # Select just our slice
    slice_frns = frns[args.offset : args.offset + args.limit]

    store = {}
    for frn in slice_frns:
        try:
            url     = f"{BASE_URL}/Firm/{frn}/Individuals"
            entries = fetch_paginated(url)
            norm    = [
                {'IRN': e.get('IRN'),
                 'Name': e.get('Name'),
                 'Status': e.get('Status'),
                 'URL': e.get('URL')}
                for e in entries if isinstance(e, dict)
            ]
            store[frn] = norm
            print(f"✅ Fetched {len(norm)} for FRN {frn}")
        except Exception as e:
            print(f"⚠️ Failed FRN {frn}: {e}")

    # Ensure output dir exists
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(store, f, indent=2, ensure_ascii=False)
    print(f"✅ Wrote {len(store)} firms to {args.output}")

if __name__=='__main__':
    main()
