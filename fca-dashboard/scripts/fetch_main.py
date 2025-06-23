#!/usr/bin/env python3
"""
scripts/fetch_main.py

Fetch the main firm records (core metadata) for all FRNs or a limited subset.
Updates data/fca_main.json by merging new entries with existing ones.
"""
import os
import json
import argparse
import requests
from rate_limiter import RateLimiter

# ─── Paths & Config ──────────────────────────────────────────────────────────
SCRIPT_DIR     = os.path.dirname(__file__)
DATA_DIR       = os.path.abspath(os.path.join(SCRIPT_DIR, "../data"))
FRNS_JSON      = os.path.join(DATA_DIR, "all_frns_with_names.json")
MAIN_JSON      = os.path.join(DATA_DIR, "fca_main.json")

# ─── FCA Register API setup ────────────────────────────────────────────────────
API_EMAIL = os.getenv("FCA_API_EMAIL")
API_KEY   = os.getenv("FCA_API_KEY")
if not API_EMAIL or not API_KEY:
    raise EnvironmentError("FCA_API_EMAIL and FCA_API_KEY must be set in the environment")

BASE_URL = "https://register.fca.org.uk/services/V0.1"
HEADERS  = {
    "Accept":       "application/json",
    "X-AUTH-EMAIL": API_EMAIL,
    "X-AUTH-KEY":   API_KEY,
}

limiter = RateLimiter()

def fetch_json(url: str) -> dict:
    limiter.wait()
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()


def fetch_main_record(frn: str) -> dict | None:
    """Fetch only the main firm record and return selected fields."""
    try:
        pkg = fetch_json(f"{BASE_URL}/Firm/{frn}")
    except Exception as e:
        print(f"⚠️  Failed fetch for FRN {frn}: {e}")
        return None

    data = pkg.get("Data") or []
    if not data:
        print(f"⚠️  No Data block for FRN {frn}")
        return None

    info = data[0]
    # Whitelist of fields to keep
    selected = {
        'frn': info.get('FRN'),
        'organisation_name': info.get('Organisation Name'),
        'status': info.get('Status'),
        'business_type': info.get('Business Type'),
        'companies_house_number': info.get('Companies House Number'),
        'exceptional_info_details': info.get('Exceptional Info Details', []),
        'system_timestamp': info.get('System Timestamp'),
        'status_effective_date': info.get('Status Effective Date'),
    }
    return selected


def main():
    parser = argparse.ArgumentParser(description="Fetch and merge main firm records")
    parser.add_argument("--limit", type=int, help="Only process first N FRNs for testing")
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)

    # Load FRN list
    with open(FRNS_JSON, "r", encoding="utf-8") as f:
        frn_items = json.load(f)
    frns = [item['frn'] for item in frn_items]
    if args.limit:
        frns = frns[: args.limit]
        print(f"🔍 Test mode: will fetch {len(frns)} FRNs")

    # Load existing main JSON store (frn → record)
    if os.path.exists(MAIN_JSON):
        with open(MAIN_JSON, "r", encoding="utf-8") as f:
            store = {item['frn']: item for item in json.load(f)}
    else:
        store = {}

    # Fetch & merge
    for frn in frns:
        rec = fetch_main_record(frn)
        if rec:
            store[frn] = rec
            print(f"✅ Fetched and stored main record for FRN {frn}")

    # Write back
    with open(MAIN_JSON, "w", encoding="utf-8") as f:
        json.dump(list(store.values()), f, indent=2, ensure_ascii=False)
    print(f"✅ Wrote {len(store)} firm records to {MAIN_JSON}")


if __name__ == "__main__":
    main()
