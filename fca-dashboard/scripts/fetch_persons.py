#!/usr/bin/env python3
"""
scripts/fetch_persons.py

Shard-based threaded fetch of core individual records:
  - Splits the full IRN list into a user-specified number of sequential shards
  - Processes each shard in turn, using the full API rate limit (optimal threads)
  - Writes per-shard JSON (fca_persons_part{n}.json) and a final merged fca_persons.json
  - Supports modes: --threads, --shards, --limit, --only-missing, --retry-failed, --fresh, --dry-run
"""

import os
import sys
import json
import math
import argparse
import requests
from threading import Lock
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from rate_limiter import RateLimiter

# ─── CLI ARGUMENTS ────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(
    description='Shard-based threaded fetch of individual records'
)
parser.add_argument('--threads',      type=int, default=5,
                    help='Number of parallel threads per shard (optimum ≃5)')
parser.add_argument('--shards',       type=int, default=1,
                    help='Number of sequential shards to split the IRNs into')
parser.add_argument('--limit',        type=int, default=None,
                    help='Cap on total IRNs to process (for quick tests)')
parser.add_argument('--only-missing', action='store_true',
                    help='Only fetch IRNs not yet present in the store')
parser.add_argument('--retry-failed', action='store_true',
                    help='Only retry IRNs that errored in the previous run')
parser.add_argument('--fresh',        action='store_true',
                    help='Ignore existing data and re-fetch all IRNs')
parser.add_argument('--dry-run',      action='store_true',
                    help='Dry run: list IRNs without making any API calls')
args = parser.parse_args()

# ─── PATHS ────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(__file__)
DATA_DIR     = os.path.abspath(os.path.join(SCRIPT_DIR, '../../docs/fca-dashboard/data'))
SEED_JSON    = os.path.join(DATA_DIR, 'fca_individuals_by_firm.json')
PERSONS_JSON = os.path.join(DATA_DIR, 'fca_persons.json')
FAILS_JSON   = os.path.join(DATA_DIR, 'fca_persons_fails.json')

# ─── FCA API SETUP ─────────────────────────────────────────────────────────────
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
limiter = RateLimiter()  # uses RL_MAX_CALLS / RL_WINDOW_S

def fetch_json(url: str) -> dict:
    """Perform a rate-limited GET and return parsed JSON, with error logs."""
    limiter.wait()
    resp = requests.get(url, headers=HEADERS, timeout=10)
    if resp.status_code == 429:
        print(f"⚠️ Rate limit exceeded (429) at {url}")
    elif resp.status_code >= 400:
        snippet = resp.text[:200].replace('\n',' ')
        print(f"⚠️ API error {resp.status_code} at {url}: {snippet!r}")
    resp.raise_for_status()
    return resp.json()

def fetch_individual_record(irn: str):
    """Fetch and normalize a single IRN record."""
    try:
        pkg = fetch_json(f"{BASE_URL}/Individuals/{irn}")
    except Exception as e:
        print(f"⚠️ Failed fetch for IRN {irn}: {e}")
        return None
    data_list = pkg.get('Data') or []
    if not data_list:
        return None
    details = data_list[0].get('Details', {})
    return {
        'irn':                  details.get('IRN'),
        'name':                 details.get('Full Name') or details.get('Commonly Used Name'),
        'status':               details.get('Individual Status'),
        'controlled_functions': details.get('Controlled Functions'),
    }

def process_irns(irns_subset, threads):
    """Threaded fetching for a subset of IRNs; returns (results_dict, failed_list)."""
    q = Queue()
    for irn in irns_subset:
        q.put(irn)

    lock = Lock()
    results = {}
    fails = []
    processed = 0
    total = len(irns_subset)

    def worker():
        nonlocal processed
        while True:
            try:
                irn = q.get_nowait()
            except Empty:
                return
            rec = fetch_individual_record(irn)
            if rec:
                results[irn] = rec
                print(f"✅ IRN {irn}: fetched")
            else:
                fails.append(irn)
                print(f"ℹ️  IRN {irn}: no data")
            with lock:
                processed += 1
                rem = total - processed
                print(f"▶️ Shard progress {processed}/{total} ({rem} remaining)")
            q.task_done()

    with ThreadPoolExecutor(max_workers=threads) as executor:
        for _ in range(threads):
            executor.submit(worker)
        q.join()

    return results, fails

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # 1) Load master IRN map
    if not os.path.exists(SEED_JSON):
        print(f"❌ Missing seed file {SEED_JSON}")
        sys.exit(1)
    with open(SEED_JSON, 'r', encoding='utf-8') as f:
        firm_map = json.load(f)

    # 2) Flatten and dedupe IRNs
    all_irns = [rec['IRN']
                for entries in firm_map.values()
                for rec in entries
                if rec.get('IRN')]
    seen = set()
    irns = [x for x in all_irns if x not in seen and not seen.add(x)]

    # 3) Load existing store unless fresh
    store = {}
    if os.path.exists(PERSONS_JSON) and not args.fresh:
        with open(PERSONS_JSON, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            store = data

    # 4) Load previous failures if retry_failed
    prev_fails = []
    if args.retry_failed and os.path.exists(FAILS_JSON):
        with open(FAILS_JSON, 'r', encoding='utf-8') as f:
            prev_fails = json.load(f)

    # 5) Apply modes
    if args.only_missing:
        irns = [i for i in irns if str(i) not in store]
    elif args.retry_failed:
        irns = prev_fails

    # 6) Apply test limit
    if args.limit:
        irns = irns[:args.limit]
        print(f"🔍 Test mode: {len(irns)} IRNs to fetch")

    total = len(irns)
    print(f"🔍 Total IRNs to process: {total} in {args.shards} shard(s)")

    # 7) Compute shard size
    shard_size = math.ceil(total / args.shards) if args.shards > 0 else total

    merged_results = {}
    all_fails = {}

    # 8) Process each shard sequentially
    for idx in range(args.shards):
        start = idx * shard_size
        subset = irns[start:start + shard_size]
        print(f"--- Shard {idx+1}/{args.shards}: {len(subset)} IRNs ---")
        if args.dry_run:
            for irn in subset:
                print(f"➡️ Would fetch IRN {irn}")
            continue
        results, fails = process_irns(subset, args.threads)
        part_file = os.path.join(DATA_DIR, f"fca_persons_part{idx+1}.json")
        with open(part_file, 'w', encoding='utf-8') as pf:
            json.dump(results, pf, indent=2)
        merged_results.update(results)
        all_fails[idx+1] = fails

    # 9) Merge & Save final store
    with open(PERSONS_JSON, 'w', encoding='utf-8') as f:
        json.dump(merged_results, f, indent=2)
    with open(FAILS_JSON, 'w', encoding='utf-8') as f:
        json.dump(all_fails, f, indent=2)

    print(f"✅ Completed: wrote {len(merged_results)} records to {PERSONS_JSON}")
    if any(all_fails.values()):
        total_fails = sum(len(lst) for lst in all_fails.values())
        print(f"⚠️ {total_fails} IRNs failed; see {FAILS_JSON}")

if __name__ == '__main__':
    main()
