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
    raise EnvironmentError("FCA_API_EMAIL and FCA_API_KEY must both be set in the environment")

BASE_URL = "https://register.fca.org.uk/services/V0.1/Firm"
HEADERS  = {
    "Accept":       "application/json",
    "X-AUTH-EMAIL": API_EMAIL,
    "X-AUTH-KEY":   API_KEY,
}

# ─── Rate limiter ───────────────────────────────────────────────────────────────
limiter = RateLimiter()

def fetch_json(url: str) -> dict:
    """GET a URL with FCA headers, returning parsed JSON."""
    limiter.wait()
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()

def fetch_firm_details(frn: str) -> dict | None:
    """Fetch one firm + sub‑resources and return a flat dict."""
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

    # Debug: show what links are available
    links_dict = info.get("Links", {})
    print(f"👀 FRN {frn} Links keys: {list(links_dict.keys())}")

    # Core fields
    out = {
        "frn":                   info.get("FRN"),
        "organisation_name":     info.get("Organisation Name"),
        "status":                info.get("Status"),
        "business_type":         info.get("Business Type"),
        "ch_number":             info.get("Companies House Number"),
    }

    def sub_data(link_key: str) -> list:
        """Fetch a sub‑resource by Links key, return its Data array (or empty)."""
        url = links_dict.get(link_key)
        if not url:
            return []
        try:
            subpkg = fetch_json(url)
            return subpkg.get("Data") or []
        except Exception as e:
            print(f"  ⚠️ Sub‑fetch {link_key} failed: {e}")
            return []

    # Permissions
    perms = sub_data("Permission")
    out["permissions"] = [
        (p.get("Permission") if isinstance(p, dict) else str(p))
        for p in perms
    ]

    # Trading names
    names = sub_data("Name")
    out["trading_names"] = [
        (n.get("Name") if isinstance(n, dict) else str(n))
        for n in names
    ]

    # Appointed Reps
    out["appointed_reps"] = sub_data("Appointed Representative")

    # Controlled Functions: find the correct key in links
    cf_key = next((k for k in links_dict if "controlled" in k.lower()), None)
    if cf_key:
        cf_data = sub_data(cf_key)
        out["controlled_functions"] = cf_data
        out["associated_persons"] = [
            entry.get("IndividualReferenceNumber")
            or entry.get("PersonId")
            or entry.get("IRN")
            for entry in cf_data if isinstance(entry, dict)
        ]
    else:
        print(f"⚠️  No controlled‑functions link found for FRN {frn}")
        out["controlled_functions"] = []
        out["associated_persons"]    = []

    # Registered address
    addr_data = sub_data("Address")
    out["address"] = addr_data[0] if addr_data else {}

    return out

def main():
    # Parse optional --limit for quick CI tests
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Only fetch this many FRNs for testing")
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)

    # Load FRN list
    with open(FRNS_JSON, "r") as f:
        frn_items = json.load(f)
    frns = [item["frn"] for item in frn_items]

    if args.limit:
        print(f"🔍 Test mode: limiting to first {args.limit} FRNs")
        frns = frns[: args.limit]

    # Fetch details for each FRN
    results = []
    for frn in frns:
        details = fetch_firm_details(frn)
        if details:
            results.append(details)

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
        print("⚠️  No data fetched; skipping CSV output")

if __name__ == "__main__":
    main()
