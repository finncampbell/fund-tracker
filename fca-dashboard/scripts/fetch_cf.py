#!/usr/bin/env python3
"""
scripts/fetch_cf.py

Fetch controlled functions (current + previous) for each firm (FRN).
Updates data/fca_cf.json by merging new entries with existing ones.
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
CF_JSON    = os.path.join(DATA_DIR, 'fca_cf.json')

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

def flatten_cf(pkg: dict) -> list[dict]:
    """Given a /Firm/{frn}/CF response payload, flatten Current & Previous entries."""
    out = []
    data = pkg.get('Data') or []
    if not data:
        return out
    block = data[0]
    for section in ('Current', 'Previous'):
        sec_map = block.get(section, {})
        for cf_code, details in sec_map.items():
            entry = {
                'section': section,
                'controlled_function': cf_code,
                'Individual Name': details.get('Individual Name'),
                'Effective Date':   details.get('Effective Date'),
                'URL':              details.get('URL'),
            }
            out.append(entry)
    return out

def main():
    parser = argparse.ArgumentParser(description='Fetch controlled functions for firms')
    parser.add_argument('--limit', type=int, help='Only process first N FRNs for testing')
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)

    # Load FRNs
    with open(FRNS_JSON, 'r', encoding='utf-8') as f:
        frns = [item['frn'] for item in json.load(f)]
    if args.limit:
        frns = frns[:args.limit]
        print(f"🔍 Test mode: will fetch CF for {len(frns)} firms")

    # Load existing CF store
    if os.path.exists(CF_JSON):
        with open(CF_JSON, 'r', encoding='utf-8') as f:
            store = json.load(f)
    else:
        store = {}

    # Fetch & merge
    for frn in frns:
        try:
            pkg = fetch_json(f"{BASE_URL}/Firm/{frn}/CF")
            entries = flatten_cf(pkg)
            store[frn] = entries
            print(f"✅ Fetched {len(entries)} CF entries for FRN {frn}")
        except Exception as e:
            print(f"⚠️  Failed to fetch CF for FRN {frn}: {e}")

    # Write back
    with open(CF_JSON, 'w', encoding='utf-8') as f:
        json.dump(store, f, indent=2, ensure_ascii=False)
    print(f"✅ Wrote controlled‑functions for {len(store)} firms to {CF_JSON}")

if __name__ == '__main__':
    main()
