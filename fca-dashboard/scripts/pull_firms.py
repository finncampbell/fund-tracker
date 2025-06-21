#!/usr/bin/env python3
import os
import json
import requests
import pandas as pd
from rate_limiter import RateLimiter

# Paths
SCRIPT_DIR   = os.path.dirname(__file__)
DATA_DIR     = os.path.abspath(os.path.join(SCRIPT_DIR, "../data"))
FRNS_JSON    = os.path.join(DATA_DIR, "all_frns_with_names.json")
OUTPUT_JSON  = os.path.join(DATA_DIR, "fca_firms.json")
OUTPUT_CSV   = os.path.join(DATA_DIR, "fca_firms.csv")

# FCA Register API (single‐resource endpoint)
API_KEY   = os.getenv("FCA_API_KEY")
if not API_KEY:
    raise EnvironmentError("FCA_API_KEY not set in environment")
BASE_URL  = "https://register.fca.org.uk/services/V0.1/Firm"

# Rate limiter instance
limiter = RateLimiter()

def fetch_firm(frn: str) -> dict | None:
    """Fetch one firm by FRN via Basic Auth, return JSON or None."""
    limiter.wait()
    url = f"{BASE_URL}/{frn}"
    try:
        resp = requests.get(url,
                            auth=(API_KEY, ""),
                            headers={"Accept": "application/json"},
                            timeout=10)
    except requests.RequestException as e:
        print(f"❌ Network error for FRN {frn}: {e}")
        return None

    if resp.status_code == 200:
        return resp.json()
    else:
        print(f"⚠️  Failed to fetch FRN {frn}: HTTP {resp.status_code}")
        return None

def main():
    # Ensure output directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    # Load your FRN→name seed list
    with open(FRNS_JSON, "r") as f:
        frn_items = json.load(f)
    frns = [item["frn"] for item in frn_items]

    # Fetch each firm
    results = []
    for frn in frns:
        data = fetch_firm(frn)
        if data:
            results.append(data)

    # Write raw JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"✅ Wrote {len(results)} firms to {OUTPUT_JSON}")

    # Flatten and write CSV
    if results:
        df = pd.json_normalize(results)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"✅ Wrote {len(df)} rows to {OUTPUT_CSV}")
    else:
        print("⚠️  No firm data fetched; skipping CSV output")

if __name__ == "__main__":
    main()
