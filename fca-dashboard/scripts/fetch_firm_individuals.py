#!/usr/bin/env python3
"""
scripts/fetch_firm_individuals.py

Fetch the list of individuals for each firm (by FRN) via the paginated
/Firm/{frn}/Individuals endpoint, but only for a slice of the FRN list
(defined by --offset and --limit).  Writes one chunk’s results to --output.
"""

import os
import json
import argparse
import requests

# ─── Import & initialize our rate limiter (enforces FCA’s 50 calls / 10s) ───
from rate_limiter import RateLimiter
limiter = RateLimiter()

# ─── Paths & Constants ───────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(__file__)
DATA_DIR   = os.path.abspath(os.path.join(SCRIPT_DIR, '../data'))
FRNS_JSON  = os.path.join(DATA_DIR, 'all_frns_with_names.json')
BASE_URL   = 'https://register.fca.org.uk/services/V0.1'
HEADERS    = {
    'Accept':       'application/json',
    'X-AUTH-EMAIL': os.getenv('FCA_API_EMAIL'),
    'X-AUTH-KEY':   os.getenv('FCA_API_KEY'),
}

# ─── Validate environment variables ───────────────────────────────────────────
if not HEADERS['X-AUTH-EMAIL'] or not HEADERS['X-AUTH-KEY']:
    raise EnvironmentError('FCA_API_EMAIL and FCA_API_KEY must be set')

def fetch_json(url: str) -> dict:
    """
    GET JSON from the FCA API, waiting according to rate limits before each call.
    Raises on HTTP errors.
    """
    limiter.wait()
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()

def fetch_paginated(url: str) -> list:
    """
    Follow the API’s pagination via Data + ResultInfo.Next, returning
    the concatenated list of all Data entries.
    """
    items = []
    next_url = url
    while next_url:
        pkg = fetch_json(next_url)
        # Append whatever’s in "Data" (or empty list if absent)
        items.extend(pkg.get('Data') or [])
        # Continue if the server provided a Next link
        next_url = pkg.get('ResultInfo', {}).get('Next')
    return items

def main():
    # ─── Parse arguments for our slice and output path ────────────────────────
    p = argparse.ArgumentParser(description='Fetch a slice of firm-individuals')
    p.add_argument('--offset', type=int, required=True,
                   help='Zero-based start index into the FRN list')
    p.add_argument('--limit',  type=int, required=True,
                   help='How many FRNs to process in this chunk')
    p.add_argument('--output', required=True,
                   help='Filepath to write this chunk’s JSON')
    args = p.parse_args()

    # ─── Load the master FRN list (all_frns_with_names.json) ──────────────────
    with open(FRNS_JSON, encoding='utf-8') as f:
        frn_items = json.load(f)
    frns = [item['frn'] for item in frn_items]

    # ─── Slice out only this chunk’s FRNs ────────────────────────────────────
    slice_frns = frns[args.offset : args.offset + args.limit]

    store = {}
    for frn in slice_frns:
        try:
            # Build the URL and fetch all pages
            url     = f"{BASE_URL}/Firm/{frn}/Individuals"
            entries = fetch_paginated(url)

            # Normalize each entry to just IRN, Name, Status, URL
            norm = [
                {
                  'IRN':    e.get('IRN'),
                  'Name':   e.get('Name'),
                  'Status': e.get('Status'),
                  'URL':    e.get('URL')
                }
                for e in entries if isinstance(e, dict)
            ]

            store[frn] = norm
            print(f"✅ Fetched {len(norm)} individuals for FRN {frn}")
        except Exception as e:
            # Log failures but continue the loop
            print(f"⚠️  Failed FRN {frn}: {e}")

    # ─── Ensure the output directory exists, then write JSON ────────────────
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(store, f, indent=2, ensure_ascii=False)
    print(f"✅ Wrote {len(store)} firms to {args.output}")

if __name__ == '__main__':
    main()
