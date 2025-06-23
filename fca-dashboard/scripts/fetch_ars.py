#!/usr/bin/env python3
"""
scripts/fetch_ars.py

Fetch appointed representatives for all FRNs or a limited subset.
Updates data/fca_ars.json by merging new entries with existing ones.
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
ARS_JSON   = os.path.join(DATA_DIR, 'fca_ars.json')

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
    limiter.wait()
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()

def main():
    parser = argparse.ArgumentParser(description='Fetch Appointed Representatives for firms')
    parser.add_argument('--limit', type=int, help='Only process first N FRNs for testing')
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)

    # Load FRN list
    with open(FRNS_JSON, 'r', encoding='utf-8') as f:
        frn_items = json.load(f)
    frns = [item['frn'] for item in frn_items]
    if args.limit:
        frns = frns[:args.limit]
        print(f"🔍 Test mode: will fetch ARs for {len(frns)} FRNs")

    # Load existing AR store
    if os.path.exists(ARS_JSON):
        with open(ARS_JSON, 'r', encoding='utf-8') as f:
            data = json.load(f)
        store = data if isinstance(data, dict) else {}
    else:
        store = {}

    # Fetch & merge
    for frn in frns:
        try:
            pkg = fetch_json(f"{BASE_URL}/Firm/{frn}/AR")
            entries = pkg.get('Data') or []
            store[frn] = entries
            print(f"✅ Fetched {len(entries)} AR entries for FRN {frn}")
        except Exception as e:
            print(f"⚠️  Failed to fetch ARs for FRN {frn}: {e}")

    # Write back
    with open(ARS_JSON, 'w', encoding='utf-8') as f:
        json.dump(store, f, indent=2, ensure_ascii=False)
    print(f"✅ Wrote AR data for {len(store)} firms to {ARS_JSON}")

if __name__ == '__main__':
    main()
