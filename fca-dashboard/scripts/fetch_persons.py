#!/usr/bin/env python3
"""
scripts/fetch_persons.py

Shard-based threaded fetch of FCA individual records, with 429 backoff and
simplified logging (only a counter per record).

Usage (GitHub Actions matrix):
  python3 fca-dashboard/scripts/fetch_persons.py \
    --threads N \
    --shards M \
    --shard-index K \
    [--limit L] \
    [--only-missing] \
    [--retry-failed] \
    [--fresh] \
    [--dry-run]
"""

import os
import sys
import json
import math
import time
import argparse
import requests
from threading import Lock
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from rate_limiter import RateLimiter

# ─── CONFIG ───────────────────────────────────────────────────────────────────
MAX_RETRIES = 5

parser = argparse.ArgumentParser(
    description='Fetch a single shard of FCA individual records'
)
parser.add_argument('--threads',      type=int, required=True,
                    help='Number of threads for this shard')
parser.add_argument('--shards',       type=int, required=True,
                    help='Total number of sequential shards')
parser.add_argument('--shard-index',  type=int, required=True,
                    help='1-based index of this shard (1…shards)')
parser.add_argument('--limit',        type=int, default=None,
                    help='Cap on total IRNs to process (testing)')
parser.add_argument('--only-missing', action='store_true',
                    help='Only fetch IRNs not yet in the store')
parser.add_argument('--retry-failed', action='store_true',
                    help='Only retry IRNs that errored previously')
parser.add_argument('--fresh',        action='store_true',
                    help='Ignore existing store and re-fetch all IRNs')
parser.add_argument('--dry-run',      action='store_true',
                    help='Dry run: list IRNs without making API calls')
args = parser.parse_args()

# ─── PATHS ────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(__file__)
DATA_DIR     = os.path.abspath(os.path.join(SCRIPT_DIR, '../../docs/fca-dashboard/data'))
SEED_JSON    = os.path.join(DATA_DIR, 'fca_individuals_by_firm.json')
PERSONS_JSON = os.path.join(DATA_DIR, f'fca_persons_part{args.shard_index}.json')
FAILS_JSON   = os.path.join(DATA_DIR, f'fca_persons_fails_part{args.shard_index}.json')

# ─── FCA API SETUP ────────────────────────────────────────────────────────────
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
    """
    Rate-limited GET → JSON.
    On 429 or 5xx, back off and retry up to MAX_RETRIES.
    """
    retries = 0
    while True:
        limiter.wait()
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
        except requests.RequestException as e:
            if retries < MAX_RETRIES:
                retries += 1
                time.sleep(5)
                continue
            raise

        code = resp.status_code
        if code == 429:
            retries += 1
            if retries > MAX_RETRIES: resp.raise_for_status()
            time.sleep(limiter.window)
            continue
        if 500 <= code < 600:
            retries += 1
            if retries > MAX_RETRIES: resp.raise_for_status()
            time.sleep(5 * retries)
            continue

        if code >= 400:
            snippet = resp.text[:200].replace('\n',' ')
            print(f"⚠️ API error {code} at {url}: {snippet!r}")
        resp.raise_for_status()
        return resp.json()

def fetch_individual_record(irn: str):
    """Fetch and normalize one IRN record."""
    try:
        pkg = fetch_json(f"{BASE_URL}/Individuals/{irn}")
    except Exception:
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

def process_irns(irns_subset):
    """Threaded fetch over a subset of IRNs, with a simple counter."""
    q = Queue()
    for irn in irns_subset: q.put(irn)
    lock = Lock()
    results, fails = {}, []
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
            else:
                fails.append(irn)
            with lock:
                processed += 1
                print(f"▶️ Shard {args.shard_index}: {processed}/{total} IRNs done")
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

    with open(SEED_JSON,'r',encoding='utf-8') as f:
        firm_map = json.load(f)
    all_irns = [rec['IRN']
                for entries in firm_map.values()
                for rec in entries
                if rec.get('IRN')]
    seen = set()
    irns = [x for x in all_irns if x not in seen and not seen.add(x)]

    # Load existing store
    store = {}
    if os.path.exists(PERSONS_JSON) and not args.fresh:
        with open(PERSONS_JSON,'r',encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            store = data

    # Retry failures
    prev_fails = []
    if args.retry_failed and os.path.exists(FAILS_JSON):
        with open(FAILS_JSON,'r',encoding='utf-8') as f:
            prev_fails = json.load(f)

    # Apply filters
    if args.only_missing:
        irns = [i for i in irns if str(i) not in store]
    elif args.retry_failed:
        irns = prev_fails

    # Limit for testing
    if args.limit:
        irns = irns[:args.limit]

    total = len(irns)
    size  = math.ceil(total / args.shards)
    idx   = args.shard_index - 1
    if idx < 0 or idx >= args.shards:
        print(f"❌ Invalid shard-index {args.shard_index}")
        sys.exit(1)
    subset = irns[idx*size:(idx+1)*size]
    print(f"🔍 Shard {args.shard_index}/{args.shards}: {len(subset)} IRNs")

    if args.dry_run:
        for irn in subset:
            print(f"➡️ Would fetch {irn}")
        return

    results, fails = process_irns(subset)

    # Write outputs
    with open(PERSONS_JSON,'w',encoding='utf-8') as pf:
        json.dump(results, pf, indent=2, ensure_ascii=False)
    with open(FAILS_JSON,'w',encoding='utf-8') as ff:
        json.dump(fails, ff, indent=2, ensure_ascii=False)
    print(f"✅ Completed shard {args.shard_index}: "
          f"{len(results)} records, {len(fails)} fails")

if __name__ == '__main__':
    main()
