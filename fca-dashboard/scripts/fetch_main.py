#!/usr/bin/env python3
import os
import json
import argparse
import requests
from rate_limiter import RateLimiter

# ─── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR     = os.path.dirname(__file__)
DATA_DIR       = os.path.abspath(os.path.join(SCRIPT_DIR, "../data"))
FRNS_JSON      = os.path.join(DATA_DIR, "all_frns_with_names.json")
MAIN_JSON      = os.path.join(DATA_DIR, "fca_main.json")

# ─── FCA Register API setup ────────────────────────────────────────────────────
API_EMAIL = os.getenv("FCA_API_EMAIL")
API_KEY   = os.getenv("FCA_API_KEY")
if not API_EMAIL or not API_KEY:
    raise EnvironmentError("FCA_API_EMAIL and FCA_API_KEY must be set")

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
    """Fetch only the main firm record and return its Data[0] dict."""
    try:
        pkg = fetch_json(f"{BASE_URL}/Firm/{frn}")
    except Exception as e:
        print(f"⚠️  Failed fetch for FRN {frn}: {e}")
        return None

    data = pkg.get("Data") or []
    if not data:
        print(f"⚠️  No Data block for FRN {frn}")
        return None

    record = data[0].copy()
    # Optionally: prune out the 'Links' key if you don't need it in main JSON
    # record.pop("Links", None)
    return record

def main():
    parser = argparse.ArgumentParser(description="Fetch and merge main firm records")
    parser.add_argument("--limit", type=int, help="Only process first N FRNs")
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)

    # Load FRN list
    with open(FRNS_JSON, "r", encoding="utf-8") as f:
        frn_items = json.load(f)
    frns = [item["frn"] for item in frn_items]
    if args.limit:
        frns = frns[: args.limit]
        print(f"🔍 Test mode: will fetch {len(frns)} FRNs")

    # Load existing main JSON store (frn → record)
    if os.path.exists(MAIN_JSON):
        with open(MAIN_JSON, "r", encoding="utf-8") as f:
            store = json.load(f)
    else:
        store = {}

    # Fetch & merge
    for frn in frns:
        rec = fetch_main_record(frn)
        if rec:
            store[frn] = rec

    # Write back
    with open(MAIN_JSON, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, ensure_ascii=False)
    print(f"✅ Wrote {len(store)} firm records to {MAIN_JSON}")

if __name__ == "__main__":
    main()
