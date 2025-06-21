#!/usr/bin/env python3
import os
import json
import requests
import pandas as pd
import argparse
from rate_limiter import RateLimiter

# ─── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(__file__)
DATA_DIR     = os.path.abspath(os.path.join(SCRIPT_DIR, "../data"))
FRNS_JSON    = os.path.join(DATA_DIR, "all_frns_with_names.json")
OUTPUT_JSON  = os.path.join(DATA_DIR, "fca_firms.json")
OUTPUT_CSV   = os.path.join(DATA_DIR, "fca_firms.csv")

# ─── FCA Register API setup ────────────────────────────────────────────────────
API_EMAIL = os.getenv("FCA_API_EMAIL")
API_KEY   = os.getenv("FCA_API_KEY")
if not API_EMAIL or not API_KEY:
    raise EnvironmentError("FCA_API_EMAIL and FCA_API_KEY must both be set")

BASE_URL = "https://register.fca.org.uk/services/V0.1/Firm"
HEADERS  = {
    "Accept":       "application/json",
    "X-AUTH-EMAIL": API_EMAIL,
    "X-AUTH-KEY":   API_KEY,
}

# ─── Rate limiter ───────────────────────────────────────────────────────────────
limiter = RateLimiter()

def fetch_json(url: str) -> dict:
    """GET a URL with our FCA headers, returning JSON or {}."""
    limiter.wait()
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()

def fetch_firm_details(frn: str) -> dict | None:
    """Fetch a firm and its sub-resources, return a flat dict or None."""
    try:
        pkg = fetch_json(f"{BASE_URL}/{frn}")
    except Exception as e:
        print(f"⚠️  Failed main fetch for FRN {frn}: {e}")
        return None

    data = pkg.get("Data", [])
    if not data:
        print(f"⚠️  No Data block for FRN {frn}")
        return None

    info = data[0]

    # Core fields from the main record
    out = {
        "frn":                  info.get("FRN"),
        "organisation_name":    info.get("Organisation Name"),
        "status":               info.get("Status"),
        "status_effective_date":info.get("Status Effective Date"),
        "business_type":        info.get("Business Type"),
        "ch_number":            info.get("Companies House Number"),
        "sys_timestamp":        info.get("System Timestamp"),
    }

    # Helper to GET any sub-resource and return its Data array
    def sub_data(key: str) -> list[dict]:
        url = info.get(key)
        if not url:
            return []
        try:
            subpkg = fetch_json(url)
            return subpkg.get("Data", [])
        except Exception as e:
            print(f"  ⚠️ Sub-fetch {key} failed: {e}")
            return []

    # Permissions list (strings)
    perms = sub_data("Permission")
    out["permissions"] = [p.get("Permission") or p.get("Type") or str(p) for p in perms]

    # Trading names
    names = sub_data("Name")
    out["trading_names"] = [n.get("Name") or n.get("OrganisationName") or str(n) for n in names]

    # Appointed Representative ↔ Principal link
    ar = sub_data("Appointed Representative")
    # e.g. AR entries contain fields like 'AR FRN' and 'Principal FRN'
    out["appointed_reps"] = ar

    # Registered address block
    addr = sub_data("Address")
    # often a single dict with address fields
    out["address"] = addr[0] if addr else {}

    return out

def main():
    # — parse optional --limit for quick tests —
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Fetch only N firms for testing")
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)

    # Load FRN list
    with open(FRNS_JSON, "r") as f:
        frn_items = json.load(f)
    frns = [item["frn"] for item in frn_items]

    if args.limit:
        print(f"🔍 Test mode: limiting to first {args.limit} FRNs")
        frns = frns[: args.limit]

    # Fetch details
    results = []
    for frn in frns:
        details = fetch_firm_details(frn)
        if details:
            results.append(details)

    # Dump JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"✅ Wrote {len(results)} firms to {OUTPUT_JSON}")

    # Flatten & dump CSV
    if results:
        df = pd.json_normalize(results)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"✅ Wrote {len(df)} rows to {OUTPUT_CSV}")
    else:
        print("⚠️  No data fetched; skipping CSV")

if __name__ == "__main__":
    main()
