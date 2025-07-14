#!/usr/bin/env python3
"""
scripts/fetch_cf.py

Threaded, sharded fetch of each individual’s Controlled-Functions history,
peeking to cost only one call for 0 or 1-page cases, and emitting a simple
progress counter.

Loads its IRNs from docs/fca-dashboard/data/fca_individuals_by_firm.json.
"""

import os
import sys
import json
import time
import math
import argparse
import requests
from threading import Lock
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from rate_limiter import RateLimiter

# ─── CLI ARGUMENTS ────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(
    description="Fetch CF data for IRNs in parallel, sharded"
)
parser.add_argument('--threads',      type=int,    help='Worker threads',    required=True)
parser.add_argument('--shards',       type=int,    help='Total shards',      required=True)
parser.add_argument('--shard-index',  type=int,    help='1-based shard idx', required=True)
parser.add_argument('--limit',        type=int,    default=None,           help='Cap IRNs for testing')
parser.add_argument('--only-missing', action='store_true',               help='Skip IRNs already in store')
parser.add_argument('--retry-failed', action='store_true',               help='Only retry previous failures')
parser.add_argument('--fresh',        action='store_true',               help='Ignore existing store')
parser.add_argument('--dry-run',      action='store_true',               help='List IRNs without API calls')
args = parser.parse_args()

# ─── PATHS ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR       = os.path.dirname(__file__)
IND_BY_FIRM_JSON = os.path.abspath(os.path.join(
    SCRIPT_DIR,
    '../../docs/fca-dashboard/data/fca_individuals_by_firm.json'
))
CF_STORE         = os.path.abspath(os.path.join(
    SCRIPT_DIR,
    f'../../docs/fca-dashboard/data/fca_cf_part{args.shard_index}.json'
))
CF_FAILS         = os.path.abspath(os.path.join(
    SCRIPT_DIR,
    f'../../docs/fca-dashboard/data/fca_cf_fails_part{args.shard_index}.json'
))

# ─── FCA API SETUP ────────────────────────────────────────────────────────────
API_EMAIL = os.getenv('FCA_API_EMAIL')
API_KEY   = os.getenv('FCA_API_KEY')
if not API_EMAIL or not API_KEY:
    raise RuntimeError("FCA_API_EMAIL and FCA_API_KEY must be set")
BASE_URL = 'https://register.fca.org.uk/services/V0.1'
HEADERS  = {
    'Accept':       'application/json',
    'X-AUTH-EMAIL': API_EMAIL,
    'X-AUTH-KEY':   API_KEY,
}
limiter = RateLimiter()

def safe_get(url: str) -> dict:
    """Rate-limited GET with simple retry on 429."""
    attempt = 0
    while True:
        limiter.wait()
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 429 and attempt < 5:
            attempt += 1
            time.sleep(limiter.window)
            continue
        resp.raise_for_status()
        return resp.json()

def fetch_cf_for_irn(irn: str) -> list:
    """Peek & paginate `/Individuals/{irn}/CF`."""
    url = f"{BASE_URL}/Individuals/{irn}/CF"
    pkg = safe_get(url)
    ri = pkg.get('ResultInfo') or {}
    total = int(ri.get('total_count') or 0)
    if total == 0:
        return []

    data = pkg.get('Data') or []
    nxt  = ri.get('Next')
    while nxt:
        pkg2 = safe_get(nxt)
        data.extend(pkg2.get('Data') or [])
        nxt = (pkg2.get('ResultInfo') or {}).get('Next')

    # Flatten Current/Previous into a single list
    out = []
    first = data[0] if data else {}
    for section in ('Previous', 'Current'):
        for role_name, vals in (first.get(section) or {}).items():
            entry = {**vals, 'role': role_name, 'when': section.lower()}
            out.append(entry)
    return out

def main():
    os.makedirs(os.path.dirname(CF_STORE), exist_ok=True)

    if not os.path.exists(IND_BY_FIRM_JSON):
        print(f"❌ Missing seed file: {IND_BY_FIRM_JSON}")
        sys.exit(1)
    with open(IND_BY_FIRM_JSON, 'r', encoding='utf-8') as f:
        mapping = json.load(f)

    # Flatten & dedupe all IRNs
    all_irns = [
        rec['IRN']
        for entries in mapping.values()
        for rec in entries
        if rec.get('IRN')
    ]
    seen = set()
    irns = [i for i in all_irns if i not in seen and not seen.add(i)]

    # Load existing store & failures
    store, prev_fails = {}, []
    if os.path.exists(CF_STORE) and not args.fresh:
        with open(CF_STORE,'r',encoding='utf-8') as f:
            store = json.load(f)
    if args.retry_failed and os.path.exists(CF_FAILS):
        with open(CF_FAILS,'r',encoding='utf-8') as f:
            prev_fails = json.load(f)

    # Apply filters
    if args.only_missing:
        irns = [i for i in irns if i not in store]
    elif args.retry_failed:
        irns = prev_fails

    if args.limit:
        irns = irns[:args.limit]

    # Compute which slice this shard processes
    total_irns = len(irns)
    per_shard   = math.ceil(total_irns / args.shards)
    start       = (args.shard_index - 1) * per_shard
    subset      = irns[start : start + per_shard]
    print(f"🔍 Shard {args.shard_index}/{args.shards} → {len(subset)} IRNs")

    if args.dry_run:
        for i in subset:
            print(f"➡️ Would fetch CF for {i}")
        return

    # Threaded fetch with only a single counter print per IRN
    q = Queue()
    for i in subset:
        q.put(i)

    lock = Lock()
    processed = 0
    results, fails = {}, []

    def worker():
        nonlocal processed
        while True:
            try:
                irn = q.get_nowait()
            except Empty:
                return
            try:
                results[irn] = fetch_cf_for_irn(irn)
            except:
                fails.append(irn)
            finally:
                with lock:
                    processed += 1
                    print(f"▶️  Processed {processed}/{len(subset)} IRNs")
                q.task_done()

    with ThreadPoolExecutor(max_workers=args.threads) as ex:
        for _ in range(args.threads):
            ex.submit(worker)
        q.join()

    # Write out this shard’s results + failures
    with open(CF_STORE, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    with open(CF_FAILS, 'w', encoding='utf-8') as f:
        json.dump(fails, f, indent=2, ensure_ascii=False)

    print(f"✅ Shard complete: {len(results)} entries, {len(fails)} failures")

if __name__ == '__main__':
    main()
