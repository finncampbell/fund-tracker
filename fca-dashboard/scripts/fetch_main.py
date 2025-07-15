#!/usr/bin/env python3
"""
Fetch Firm Details from FCA, in deduped, sharded, threaded batches under a rate limit.
Writes each shard to fca-dashboard/data/fca_main_shard_<shard-index>.json
"""
import os
import json
import argparse
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from rate_limiter import RateLimiter

BASE_URL  = "https://register.fca.org.uk/services/V0.1/Firm"
SEED_PATH = "docs/fca-dashboard/data/all_frns_with_names.json"
OUT_DIR   = "fca-dashboard/data"
API_KEY   = os.getenv("FCA_API_KEY")
API_EMAIL = os.getenv("FCA_API_EMAIL")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--shards",      type=int, default=1, help="Total parallel shards")
    p.add_argument("--shard-index", type=int, default=1, help="1-based index of this shard")
    p.add_argument("--threads",     type=int, default=10, help="Threads per shard")
    p.add_argument("--only-missing", action="store_true", help="Fetch only FRNs not yet in output")
    return p.parse_args()

def fetch_firm(frn, limiter):
    limiter.wait()
    url = f"{BASE_URL}/{frn}"
    headers = {
        "x-auth-key": API_KEY,
        "x-auth-email": API_EMAIL,
        "Content-Type": "application/json"
    }
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json().get("Data", [])
    if not data:
        return frn, None
    info = data[0]
    return frn, {
        "frn": str(frn),
        "organisation_name": info.get("Organisation Name"),
        "status": info.get("Status"),
        "status_effective_date": info.get("Status Effective Date"),
        "business_type": info.get("Business Type"),
        "system_timestamp": info.get("System Timestamp"),
        "companies_house_number": info.get("Companies House Number"),
        "exceptions": info.get("Exceptional Info Details", []),
        "_links": {
            "names":        info.get("Names"),
            "individuals":  info.get("Individuals"),
            "requirements": info.get("Requirements"),
            "permissions":  info.get("Permissions"),
            "passport":     info.get("Passport"),
            "regulators":   info.get("Regulators"),
            "appointed_reps": info.get("Appointed Representative"),
            "address":      info.get("Address"),
            "waivers":      info.get("Waivers"),
            "exclusions":   info.get("Exclusions"),
            "disciplinary_history": info.get("DisciplinaryHistory"),
        }
    }

def main():
    args = parse_args()

    # Load & dedupe FRN list
    raw = json.load(open(SEED_PATH, encoding="utf-8"))
    frns = [str(x["frn"]) for x in raw]
    seen = set()
    unique_frns = [x for x in frns if x not in seen and not seen.add(x)]

    # Determine this shard’s slice
    slice_frns = [
        frn for idx, frn in enumerate(unique_frns)
        if (idx % args.shards) + 1 == args.shard_index
    ]

    # Prepare output path for this shard
    out_path = os.path.join(OUT_DIR, f"fca_main_shard_{args.shard_index}.json")
    existing = {}
    if args.only_missing and os.path.exists(out_path):
        existing = json.load(open(out_path, encoding="utf-8"))
    store = existing.copy()

    # Each shard now runs sequentially, so use full rate limit
    limiter = RateLimiter(max_calls=45, window_s=10)

    # Threaded fetch
    with ThreadPoolExecutor(max_workers=args.threads) as exe:
        futures = {exe.submit(fetch_firm, frn, limiter): frn for frn in slice_frns}
        for fut in as_completed(futures):
            frn, record = fut.result()
            if record:
                store[frn] = record
                print(f"✅ {frn}")

    # Write this shard’s results
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, ensure_ascii=False)
    print(f"🎉 Wrote {len(store)} records to {out_path}")

if __name__ == "__main__":
    main()
