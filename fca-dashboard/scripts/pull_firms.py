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
    limiter.wait()
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()

def fetch_firm_details(frn: str) -> dict | None:
    """Fetch one firm + all its sub‑resources, returning a flat dict."""
    try:
        pkg = fetch_json(f"{BASE_URL}/{frn}")
    except Exception as e:
        print(f"⚠️  Failed main fetch for FRN {frn}: {e}")
        return None

    data = pkg.get("Data") or []
    if not data:
        print(f"⚠️  No Data block for FRN {frn}")
        return None

    info = data[0]

    # Print out the keys so you can spot the correct “controlled functions” property
    print(f"👀 FRN {frn} info keys: {list(info.keys())}")

    # Core fields
    out = {
        "frn":                   info.get("FRN"),
        "organisation_name":     info.get("Organisation Name"),
        "status":                info.get("Status"),
        "business_type":         info.get("Business Type"),
    }

    def sub_data(key: str) -> list:
        url = info.get(key)
        if not url:
            return []
        try:
            subpkg = fetch_json(url)
            return subpkg.get("Data") or []
        except Exception as e:
            print(f"  ⚠️ Sub‑fetch {key} failed: {e}")
            return []

    # Permissions & names (as before)…
    perms = sub_data("Permission")
    out["permissions"] = [p.get("Permission") if isinstance(p, dict) else str(p) for p in perms]

    # [ …trading_names and appointed_reps code unchanged… ]

    # --- DYNAMIC CONTROLLED FUNCTIONS ---
    # Find whichever key contains “controlled” (case‑insensitive)
    cf_key = next((k for k in info if "controlled" in k.lower()), None)
    if cf_key:
        cf_data = sub_data(cf_key)
        out["controlled_functions"] = cf_data
        out["associated_persons"] = [
            # Try the most common IRN field names
            entry.get("IndividualReferenceNumber")
            or entry.get("PersonId")
            or entry.get("IRN")
            for entry in cf_data if isinstance(entry, dict)
        ]
    else:
        print(f"⚠️  No controlled‑functions link found for FRN {frn}")
        out["controlled_functions"] = []
        out["associated_persons"]    = []

    return out

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Fetch only N firms for testing")
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)
    frn_items = json.load(open(FRNS_JSON))
    frns = [item["frn"] for item in frn_items]
    if args.limit:
        frns = frns[: args.limit]
        print(f"🔍 Test mode: first {args.limit} FRNs only")

    results = []
    for frn in frns:
        details = fetch_firm_details(frn)
        if details:
            results.append(details)

    # Write JSON & CSV as before…
    with open(OUTPUT_JSON, "w") as f:
        json.dump(results, f, indent=2)
    if results:
        pd.json_normalize(results).to_csv(OUTPUT_CSV, index=False)

if __name__ == "__main__":
    main()
