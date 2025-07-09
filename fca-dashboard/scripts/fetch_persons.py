#!/usr/bin/env python3
"""
scripts/fetch_persons.py

Shard-based threaded fetch of individual records for FCA:
  • Processes a single shard per invocation (use with GitHub Actions matrix)
  • Users specify --threads, --shards, and --shard-index (1-based)
  • Optional flags: --limit, --only-missing, --retry-failed, --fresh, --dry-run
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
    description='Fetch a single shard of FCA individual records'
)
parser.add_argument('--threads',      type=int, required=True,
                    help='Number of threads to use for this shard')
parser.add_argument('--shards',       type=int, required=True,
                    help='Total number of sequential shards')
parser.add_argument('--shard-index',  type=int, required=True,
                    help='1-based index of which shard to process')
parser.add_argument('--limit',        type=int, default=None,
                    help='Cap on total IRNs to process (for quick tests)')
parser.add_argument('--only-missing', action='store_true',
                    help='Only fetch IRNs not yet in the store')
parser.add_argument('--retry-failed', action='store_true',
                    help='Only retry IRNs that errored in previous run')
parser.add_argument('--fresh',        action='store_true',
                    help='Ignore existing data and re-fetch all IRNs')
parser.add_argument('--dry-run',      action='store_true',
                    help='Dry run: list IRNs without making any API calls')
args = parser.parse_args()

# ─── PATHS ────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(__file__)
DATA_DIR     = os.path.abspath(os.path.join(SCRIPT_DIR, '../../docs/fca-dashboard/data'))
SEED_JSON    = os.path.join(DATA_DIR, 'fca_individuals_by_firm.json')
PERSONS_JSON = os.path.join(DATA_DIR, f'fca_persons_part{args.shard_index}.json')
FAILS_JSON   = os.path.join(DATA_DIR, f'fca_persons_fails_part{args.shard_index}.json')

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
limiter = RateLimiter()

def fetch_json(url: str) -> dict:
    """Rate-limited GET → parsed JSON, with error logging."""
    limiter.wait()
    resp = requests.get(url, headers=HEADERS, timeout=10)
    if resp.status_code >= 400:
        snippet = resp.text[:200].replace('\n',' ')
        print(f"⚠️ API error {resp.status_code} at {url}: {snippet!r}")
    resp.raise_for_status()
    return resp.json()

def fetch_individual_record(irn: str):
    """Fetch core individual record for an IRN."""
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
    """Threaded fetch for a given IRN subset."""
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
                print(f"ℹ️ IRN {irn}: no data")
            with lock:
                processed += 1
                print(f"▶️ Shard {args.shard_index}: {processed}/{total}")
            q.task_done()

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        for _ in range(args.threads):
            executor.submit(worker)
        q.join()

    return results, fails

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(SEED_JSON):
        print(f"❌ Missing seed file {SEED_JSON}")
        sys.exit(1)
    with open(SEED_JSON, 'r', encoding='utf-8') as f:
        firm_map = json.load(f)

    all_irns = [rec['IRN']
                for entries in firm_map.values()
                for rec in entries
                if rec.get('IRN')]
    seen = set()
    irns = [x for x in all_irns if x not in seen and not seen.add(x)]

    store = {}
    if os.path.exists(PERSONS_JSON) and not args.fresh:
        with open(PERSONS_JSON, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            store = data

    prev_fails = []
    if args.retry_failed and os.path.exists(FAILS_JSON):
        with open(FAILS_JSON, 'r', encoding='utf-8') as f:
            prev_fails = json.load(f)

    if args.only_missing:
        irns = [i for i in irns if str(i) not in store]
    elif args.retry_failed:
        irns = prev_fails

    if args.limit:
        irns = irns[:args.limit]

    total = len(irns)
    size = math.ceil(total / args.shards) if args.shards > 0 else total
    idx = args.shard_index - 1
    if idx < 0 or idx >= args.shards:
        print(f"❌ Invalid shard-index {args.shard_index}")
        sys.exit(1)

    subset = irns[idx*size:(idx+1)*size]
    print(f"🔍 Shard {args.shard_index}/{args.shards}: {len(subset)} IRNs")
    if args.dry_run:
        for irn in subset:
            print(f"➡️ Would fetch IRN {irn}")
        return

    results, fails = process_irns(subset, args.threads)
    with open(PERSONS_JSON, 'w', encoding='utf-8') as pf:
        json.dump(results, pf, indent=2)
    with open(FAILS_JSON, 'w', encoding='utf-8') as ff:
        json.dump(fails, ff, indent=2)
    print(f"✅ Wrote {len(results)} records and {len(fails)} fails for shard {args.shard_index}")

if __name__ == '__main__':
    main()
