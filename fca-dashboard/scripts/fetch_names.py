#!/usr/bin/env python3
"""
scripts/fetch_names.py

Fetch trading/other names (alternate names) for all FRNs or a limited subset.
Updates data/fca_names.json by merging new entries with existing ones.
"""
import os
import json
import argparse
import requests
from rate_limiter import RateLimiter

# ─── Paths & Config ──────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(__file__)
DATA_DIR     = os.path.abspath(os.path.join(SCRIPT_DIR, '../data'))
FRNS_JSON    = os.path.join(DATA_DIR, 'all_frns_with_names.json')
NAMES_JSON   = os.path.join(DATA_DIR, 'fca_names.json')

# ─── FCA Register API setup ───────────────────────────────────────────────────
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
    """GET a URL with FCA headers, returning parsed JSON."""
    limiter.wait()
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser(description='Fetch trading/other names for firms')
    parser.add_argument('--limit', type=int, help='Only process first N FRNs for testing')
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)

    # Load FRN list
    with open(FRNS_JSON, 'r', encoding='utf-8') as f:
        frn_items = json.load(f)
    frns = [item['frn'] for item in frn_items]
    if args.limit:
        frns = frns[: args.limit]
        print(f"🔍 Test mode: will fetch names for {len(frns)} FRNs")

    # Load existing names store
    if os.path.exists(NAMES_JSON):
        with open(NAMES_JSON, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            store = data
        else:
            store = {}
    else:
        store = {}

    # Fetch & merge
    for frn in frns:
        try:
            pkg = fetch_json(f"{BASE_URL}/Firm/{frn}/Names")
            entries = pkg.get('Data') or []
            # Normalize to list of strings
            names = [e.get('Name') if isinstance(e, dict) else str(e) for e in entries]
            store[frn] = names
            print(f"✅ Fetched {len(names)} names for FRN {frn}")
        except Exception as e:
            print(f"⚠️  Failed to fetch names for FRN {frn}: {e}")

    # Write back
    with open(NAMES_JSON, 'w', encoding='utf-8') as f:
        json.dump(store, f, indent=2, ensure_ascii=False)
    print(f"✅ Wrote names for {len(store)} firms to {NAMES_JSON}")

if __name__ == '__main__':
    main()
