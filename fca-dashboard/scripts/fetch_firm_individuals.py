#!/usr/bin/env python3
"""
scripts/fetch_firm_individuals.py

Fetch paginated “Individuals” entries for a slice of FRNs, supporting:
  • --offset: start index into the master FRN list
  • --limit:  max number of FRNs to process (omit for full slice)
  • --output: path to write this chunk’s JSON

Rate limiting uses RL_MAX_CALLS / RL_WINDOW_S from environment (defaults to 50/10).
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

# Initialize the rate limiter, which reads RL_MAX_CALLS and RL_WINDOW_S env-vars
limiter = RateLimiter()


def fetch_json(url: str) -> dict:
    """
    Perform a GET to the given URL with rate limiting.
    Returns the parsed JSON payload.
    """
    limiter.wait()
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()


def main():
    # ─── CLI Arguments ────────────────────────────────────────────────────────
    parser = argparse.ArgumentParser(
        description='Fetch paginated individual entries for a slice of FRNs'
    )
    parser.add_argument(
        '--offset',
        type=int,
        required=True,
        help='Start index into the master FRN list'
    )
    parser.add_argument(
        '--limit',
        type=int,
        required=False,
        default=None,
        help='Max number of FRNs to process (omit for full slice)'
    )
    parser.add_argument(
        '--output',
        type=str,
        required=True,
        help='File path to write this chunk’s JSON'
    )
    args = parser.parse_args()

    # ─── Load master FRN list ──────────────────────────────────────────────────
    if not os.path.exists(FRN_LIST_FILE):
        raise FileNotFoundError(f"Missing {FRN_LIST_FILE}: run update_frn_list.py first")
    with open(FRN_LIST_FILE, 'r', encoding='utf-8') as f:
        frns = [entry['frn'] for entry in json.load(f)]

    # ─── Compute this worker’s slice ──────────────────────────────────────────
    start = args.offset
    end   = start + args.limit if args.limit is not None else None
    slice_frns = frns[start:end]
    print(f"🔍 Processing FRNs[{start}:{'' if end is None else end}] → {len(slice_frns)} firms")

    # Ensure output directory exists
    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    merged = {}

    # ─── Fetch & normalize per FRN ───────────────────────────────────────────
    for frn in slice_frns:
        url = f"{BASE_URL}/Firm/{frn}/Individuals"
        all_recs = []

        # Page through results
        while url:
            pkg = fetch_json(url)

            # ─── FIX: ensure Data is iterable even if API returns null ───
            data_list = pkg.get('Data') or []
            all_recs.extend(data_list)

            # Grab the next page URL, or None if done
            url = pkg.get('ResultInfo', {}).get('Next')

        # Normalize each record to essential fields
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

    # ─── Write this chunk’s output ────────────────────────────────────────────
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    print(f"✅ Wrote {len(merged)} FRN entries to {args.output}")


if __name__ == '__main__':
    main()
