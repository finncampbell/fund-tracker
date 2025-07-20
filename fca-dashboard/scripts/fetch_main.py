#!/usr/bin/env python3
"""
Fetch Firm Details from FCA, in deduped, sharded, threaded batches under a rate limit.
Reads FRNs from the seed list at docs/fca-dashboard/data/all_frns_with_names.json.
Writes each shard to fca-dashboard/data/fca_main_shard_<shard-index>.json.
Modes:
  --only-missing : fetch FRNs not yet in merged fca_main.json
  --only-errors  : fetch FRNs whose existing record in merged fca_main.json has an "error" key
  (default) full  : fetch all FRNs unconditionally
Always returns a record per FRN, with "error" field if fetch failed.
"""
import os
import json
import argparse
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from rate_limiter import RateLimiter
from threading import Lock

# Paths
BASE_URL    = "https://register.fca.org.uk/services/V0.1/Firm"
SEED_PATH   = "docs/fca-dashboard/data/all_frns_with_names.json"
MERGED_PATH = "fca-dashboard/data/fca_main.json"
OUT_DIR     = "fca-dashboard/data"
API_KEY     = os.getenv("FCA_API_KEY")
API_EMAIL   = os.getenv("FCA_API_EMAIL")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--shards",      type=int, default=1, help="Total parallel shards")
    p.add_argument("--shard-index", type=int, default=1, help="1-based index of this shard")
    p.add_argument("--threads",     type=int, default=5, help="Threads per shard")
    p.add_argument("--only-missing", action="store_true", help="Fetch only FRNs not yet in merged output")
    p.add_argument("--only-errors",  action="store_true", help="Fetch only FRNs with existing 'error' in merged output")
    return p.parse_args()

def fetch_firm(frn, limiter):
    """Fetch one FRN with retry on 429, always returns dict (with 'error' if any)."""
    url = f"{BASE_URL}/{frn}"
    headers = {"x-auth-key": API_KEY, "x-auth-email": API_EMAIL, "Content-Type": "application/json"}
    backoff = 1
    for attempt in range(1, 4):
        limiter.wait()
        try:
            resp = requests.get(url, headers=headers, timeout=10)
        except Exception as e:
            return frn, {"frn": str(frn), "error": f"Network error: {e}"}
        if resp.status_code == 429:
            time.sleep(backoff)
            backoff *= 2
            continue
        if resp.status_code == 403:
            return frn, {"frn": str(frn), "error": "403 Forbidden"}
        if resp.status_code >= 400:
            return frn, {"frn": str(frn), "error": f"HTTP {resp.status_code} {resp.reason}"}
        break
    else:
        return frn, {"frn": str(frn), "error": "Gave up after 3 retries"}

    data = resp.json().get("Data", [])
    if not data:
        return frn, {"frn": str(frn), "error": "No data in response"}

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
            "names":           info.get("Names"),
            "individuals":     info.get("Individuals"),
            "requirements":    info.get("Requirements"),
            "permissions":     info.get("Permissions"),
            "passport":        info.get("Passport"),
            "regulators":      info.get("Regulators"),
            "appointed_reps":  info.get("Appointed Representative"),
            "address":         info.get("Address"),
            "waivers":         info.get("Waivers"),
            "exclusions":      info.get("Exclusions"),
            "disciplinary_history": info.get("DisciplinaryHistory"),
        }
    }

def main():
    args = parse_args()

    # 1) Load seed list
    raw = json.load(open(SEED_PATH, encoding="utf-8"))
    frns = [str(x["frn"]) for x in raw]
    seen = set(); unique_frns = [f for f in frns if f not in seen and not seen.add(f)]
    print(f"🔍 Loaded {len(unique_frns)} unique FRNs from {SEED_PATH}")

    merged = {}
    if os.path.exists(MERGED_PATH):
        merged = json.load(open(MERGED_PATH, encoding="utf-8"))

    # 2) Apply mode filters
    if args.only_missing:
        missing = [f for f in unique_frns if f not in merged]
        print(f"🗂  {len(missing)} missing FRNs (not in {MERGED_PATH})")
        unique_frns = missing
    elif args.only_errors:
        errored = [f for f, rec in merged.items() if isinstance(rec, dict) and "error" in rec]
        print(f"🔄  {len(errored)} FRNs with errors to retry")
        unique_frns = errored
    # else full sweep: unique_frns remains unchanged

    # 3) Shard selection
    slice_frns = [f for idx, f in enumerate(unique_frns) if (idx % args.shards) + 1 == args.shard_index]
    total = len(slice_frns)
    print(f"🚀 Shard {args.shard_index}/{args.shards} has {total} FRNs to fetch")

    # 4) Prepare output store
    out_path = os.path.join(OUT_DIR, f"fca_main_shard_{args.shard_index}.json")
    existing = {}
    if args.only_missing or args.only_errors:
        if os.path.exists(out_path):
            existing = json.load(open(out_path, encoding="utf-8"))
    store = existing.copy()

    # 5) Rate limiter
    limiter = RateLimiter(max_calls=45, window_s=10)

    # 6) Threaded fetch with progress
    lock = Lock(); completed = 0
    def task(f):
        nonlocal completed
        key, rec = fetch_firm(f, limiter)
        with lock:
            completed += 1
            print(f"Progress: {completed}/{total} fetched (shard {args.shard_index})")
        store[key] = rec
    with ThreadPoolExecutor(max_workers=args.threads) as exe:
        list(exe.map(task, slice_frns))

    # 7) Write shard output
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, ensure_ascii=False)
    print(f"✅ Shard {args.shard_index} wrote {len(store)} records to {out_path}")

if __name__ == "__main__":
    main()
