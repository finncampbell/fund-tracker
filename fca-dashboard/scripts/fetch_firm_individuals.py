#!/usr/bin/env python3
"""
scripts/fetch_firm_individuals.py

Fetch paginated “Individuals” for each FCA firm (by FRN), supporting chunked
slices via --offset/--limit, with built-in rate limiting.
"""

import os
import json
import argparse
import requests
from rate_limiter import RateLimiter

# ─── Paths & Config ──────────────────────────────────────────────────────────
SCRIPT_DIR    = os.path.dirname(__file__)
DATA_DIR      = os.path.abspath(os.path.join(SCRIPT_DIR, '../data'))
FRN_LIST_FILE = os.path.join(DATA_DIR, 'all_frns_with_names.json')

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

def main():
    parser = argparse.ArgumentParser(
        description='Fetch paginated individual entries for a slice of FRNs'
    )
    parser.add_argument(
        '--offset',
        type=int,
        required=True,
        help='Start index into the FRN list to begin processing'
    )
    parser.add_argument(
        '--limit',
        type=int,
        required=False,      # ← Now optional
        default=None,        # ← None means “process until end of list”
        help='Max number of FRNs to process from the offset (omit for full slice)'
    )
    parser.add_argument(
        '--output',
        type=str,
        required=True,
        help='File path to write this chunk’s JSON (FRN→individuals map)'
    )
    args = parser.parse_args()

    # Ensure the FRN list exists
    if not os.path.exists(FRN_LIST_FILE):
        raise RuntimeError(f"Missing {FRN_LIST_FILE}: run update_frn_list.py first")

    # Load the full FRN list
    with open(FRN_LIST_FILE, 'r', encoding='utf-8') as f:
        frns = [entry['frn'] for entry in json.load(f)]

    # Compute the slice for this worker
    start = args.offset
    end   = start + args.limit if args.limit is not None else None
    slice_frns = frns[start:end]
    print(f"🔍 Processing FRNs[{start}:{'' if end is None else end}] → {len(slice_frns)} firms")

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    merged = {}

    # Fetch each FRN’s individuals, paging through ResultInfo.Next
    for frn in slice_frns:
        url = f"{BASE_URL}/Firm/{frn}/Individuals"
        all_recs = []
        while url:
            pkg = fetch_json(url)
            all_recs.extend(pkg.get('Data', []))
            # Follow pagination
            url = pkg.get('ResultInfo', {}).get('Next', None)

        # Normalize to simple list of {IRN, Name, Status, URL}
        normalized = [
            {
                'IRN':    rec.get('IRN'),
                'Name':   rec.get('Name'),
                'Status': rec.get('Status'),
                'URL':    rec.get('URL')
            }
            for rec in all_recs
        ]
        merged[str(frn)] = normalized
        print(f"✅ FRN {frn}: fetched {len(normalized)} individuals")

    # Write out this chunk’s map
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    print(f"✅ Wrote {len(merged)} FRN entries to {args.output}")

if __name__ == '__main__':
    main()
