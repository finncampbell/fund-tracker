#!/usr/bin/env python3
"""
scripts/fetch_firm_individuals.py

Fetch the list of individuals for each firm (FRN) via the paginated /Firm/{frn}/Individuals endpoint.
Supports chunked runs via --offset and --limit so the full list can be split across multiple jobs.
Updates data/fca_individuals_by_firm.json by merging new entries with existing ones.
"""
import os
import json
import argparse
import requests
from rate_limiter import RateLimiter

# ─── Paths & Config ──────────────────────────────────────────────────────────
SCRIPT_DIR     = os.path.dirname(__file__)
DATA_DIR       = os.path.abspath(os.path.join(SCRIPT_DIR, '../data'))
FRNS_JSON      = os.path.join(DATA_DIR, 'all_frns_with_names.json')
OUT_JSON       = os.path.join(DATA_DIR, 'fca_individuals_by_firm.json')

# ─── FCA Register API setup ────────────────────────────────────────────────────
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
    """Follow pagination via ResultInfo.Next, collecting all Data entries"""
    items = []
    next_url = url
    while next_url:
        pkg = fetch_json(next_url)
        data = pkg.get('Data') or []
        items.extend(data)
        ri = pkg.get('ResultInfo', {})
        next_url = ri.get('Next')
    return items

def main():
    parser = argparse.ArgumentParser(description='Fetch firm individuals for each FRN')
    parser.add_argument('--limit',  type=int, help='Only process N FRNs')
    parser.add_argument('--offset', type=int, default=0, help='Skip first N FRNs')
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)

    # Load FRN list
    with open(FRNS_JSON, 'r', encoding='utf-8') as f:
        frn_items = json.load(f)
    all_frns = [item['frn'] for item in frn_items]

    # Slice for this chunk
    start = args.offset
    end   = args.offset + args.limit if args.limit else None
    frns  = all_frns[start:end]
    print(f"⏱️  Processing FRNs[{start}:{end or 'end'}] → {len(frns)} firms")

    # Load existing store
    if os.path.exists(OUT_JSON):
        with open(OUT_JSON, 'r', encoding='utf-8') as f:
            data = json.load(f)
        store = data if isinstance(data, dict) else {}
    else:
        store = {}

    # Fetch & merge
    for frn in frns:
        try:
            url     = f"{BASE_URL}/Firm/{frn}/Individuals"
            entries = fetch_paginated(url)
            norm    = []
            for e in entries:
                if isinstance(e, dict):
                    norm.append({
                        'IRN':    e.get('IRN'),
                        'Name':   e.get('Name'),
                        'Status': e.get('Status'),
                        'URL':    e.get('URL'),
                    })
            store[frn] = norm
            print(f"✅ Fetched {len(norm)} individuals for FRN {frn}")
        except Exception as e:
            print(f"⚠️  Failed FRN {frn}: {e}")

    # Write back
    with open(OUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(store, f, indent=2, ensure_ascii=False)
    print(f"✅ Wrote individuals for {len(store)} firms to {OUT_JSON}")

if __name__ == '__main__':
    main()
