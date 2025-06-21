#!/usr/bin/env python3
import os
import json
import requests
import pandas as pd
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
    raise EnvironmentError(
        "FCA_API_EMAIL and FCA_API_KEY must both be set in the environment"
    )

BASE_URL = "https://register.fca.org.uk/services/V0.1/Firm"
HEADERS = {
    "Accept":        "application/json",
    "X-AUTH-EMAIL":  API_EMAIL,
    "X-AUTH-KEY":    API_KEY,
}

# ─── Rate limiter ───────────────────────────────────────────────────────────────
limiter = RateLimiter()

def fetch_firm(frn: str) -> dict | None:
    """Fetch one firm by FRN, return JSON or None on failure."""
    limiter.wait()
    url = f"{BASE_URL}/{frn}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        print(f"⚠️  Failed to fetch FRN {frn}: HTTP {resp.status_code}")
    except requests.RequestException as e:
        print(f"❌ Network error for FRN {frn}: {e}")
    return None

def main():
    # 1) Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    # 2) Load your FRN→name list
    with open(FRNS_JSON, "r") as f:
        frn_items = json.load(f)
    frns = [item["frn"] for item in frn_items]

    # 3) Fetch each firm
    results = []
    for frn in frns:
        data = fetch_firm(frn)
        if data:
            results.append(data)

    # 4) Write raw JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"✅ Wrote {len(results)} firms to {OUTPUT_JSON}")

    # 5) Write flattened CSV
    if results:
        df = pd.json_normalize(results)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"✅ Wrote {len(df)} rows to {OUTPUT_CSV}")
    else:
        print("⚠️  No firm data fetched; skipping CSV output")

if __name__ == "__main__":
    main()
