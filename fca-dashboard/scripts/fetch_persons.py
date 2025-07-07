#!/usr/bin/env python3
"""
scripts/fetch_persons.py

Threaded fetch of core individual records (IRN, Name, Status, Controlled Functions URL)
for all discovered IRNs in parallel threads, with flexible modes:

  --threads N       Number of worker threads (default 5)
  --limit M         Cap on IRNs to process (for quick tests)
  --only-missing    Only fetch IRNs not yet present in the local store
  --retry-failed    Only retry IRNs that failed in the previous run
  --fresh           Ignore existing store and re-fetch all IRNs
  --dry-run         List IRNs without making any API calls
"""

import os
import json
import argparse
import requests
from threading import Lock
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, as_completed
from rate_limiter import RateLimiter

# ─── CLI Arguments ──────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(
    description='Fetch and merge individual records in parallel with modes'
)
parser.add_argument('--threads',      type=int,    default=5,
                    help='Number of parallel worker threads')
parser.add_argument('--limit',        type=int,    default=None,
                    help='Optional cap on total IRNs to process (testing)')
parser.add_argument('--only-missing', action='store_true',
                    help='Only fetch IRNs not yet present in the store')
parser.add_argument('--retry-failed', action='store_true',
                    help='Only retry IRNs that errored in the previous run')
parser.add_argument('--fresh',        action='store_true',
                    help='Ignore existing data and re-fetch all IRNs')
parser.add_argument('--dry-run',      action='store_true',
                    help='Dry run: list IRNs without API calls')
args = parser.parse_args()

# ─── Paths & Files ──────────────────────────────────────────────────────────
SCRIPT_DIR    = os.path.dirname(__file__)
DATA_DIR      = os.path.abspath(os.path.join(SCRIPT_DIR, '../data'))
IND_BY_FIRM   = os.path.join(DATA_DIR, 'fca_individuals_by_firm.json')
PERSONS_JSON  = os.path.join(DATA_DIR, 'fca_persons.json')
FAILS_JSON    = os.path.join(DATA_DIR, 'fca_persons_fails.json')

# ─── API Setup ───────────────────────────────────────────────────────────────
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
    """Rate-limited GET → parsed JSON, with explicit FCA API error logging."""
    limiter.wait()
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Network error when calling {url}: {e}")
        raise

    code = resp.status_code
    if code == 429:
        print(f"⚠️  FCA API rate limit hit (429) for URL: {url}")
    elif code >= 400:
        snippet = resp.text[:200].replace('\n', ' ')
        print(f"⚠️  FCA API error {code} for URL: {url}: {snippet!r}")

    resp.raise_for_status()
    return resp.json()

def fetch_individual_record(irn: str) -> dict | None:
    """
    Fetch core individual record for an IRN and return selected fields.
    """
    try:
        pkg = fetch_json(f"{BASE_URL}/Individuals/{irn}")
    except Exception as e:
        print(f"⚠️  Failed fetch for IRN {irn}: {e}")
        return None

    data_list = pkg.get('Data') or []
    if not data_list:
        print(f"ℹ️  No data for IRN {irn}")
        return None

    details = data_list[0].get('Details', {})
    return {
        'irn':                  details.get('IRN'),
        'name':                 details.get('Full Name') or details.get('Commonly Used Name'),
        'status':               details.get('Individual Status'),
        'controlled_functions': details.get('Controlled Functions'),
    }

def main():
    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    # 1) Load IRNs from the firm-individuals map
    if not os.path.exists(IND_BY_FIRM):
        print(f"❌ Missing {IND_BY_FIRM}: run fetch_firm_individuals.py first")
        return
    with open(IND_BY_FIRM, 'r', encoding='utf-8') as f:
        firm_map = json.load(f)

    all_irns = []
    for entries in firm_map.values():
        for rec in entries:
            irn = rec.get('IRN')
            if irn:
                all_irns.append(irn)
    # Deduplicate, preserving order
    seen = set()
    irns = [x for x in all_irns if x not in seen and not seen.add(x)]

    # 2) Load existing store & previous failures
    store = {}
    if os.path.exists(PERSONS_JSON) and not args.fresh:
        with open(PERSONS_JSON, 'r', encoding='utf-8') as f:
            existing = json.load(f)
        if isinstance(existing, dict):
            store = existing

    fails = []
    if args.retry_failed and os.path.exists(FAILS_JSON):
        with open(FAILS_JSON, 'r', encoding='utf-8') as f:
            fails = json.load(f)

    # 3) Apply filters
    if args.only_missing:
        irns = [i for i in irns if str(i) not in store]
    elif args.retry_failed:
        irns = fails

    # 4) Quick-test cap
    if args.limit:
        irns = irns[:args.limit]
        print(f"🔍 Test mode: fetching {len(irns)} IRNs")

    total = len(irns)
    print(f"🔍 Starting threaded fetch: {total} IRNs "
          f"(threads={args.threads}, fresh={args.fresh}, dry_run={args.dry_run})")

    # 5) Dry-run: list and exit
    if args.dry_run:
        for i in irns:
            print(f"➡️  Would fetch IRN {i}")
        return

    # 6) Threaded fetch setup
    q = Queue()
    for i in irns:
        q.put(i)
    lock = Lock()
    processed = 0
    results   = {}
    new_fails = []

    def worker():
        nonlocal processed
        while True:
            try:
                irn = q.get_nowait()
            except Empty:
                return

            try:
                rec = fetch_individual_record(irn)
                if rec:
                    results[irn] = rec
                    print(f"✅ IRN {irn}: fetched")
            except Exception as e:
                new_fails.append(irn)
                print(f"⚠️  IRN {irn}: error {e}")
            finally:
                with lock:
                    processed += 1
                    rem = total - processed
                    print(f"▶️  Processed {processed}/{total} ({rem} remaining)")
                q.task_done()

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = [executor.submit(worker) for _ in range(args.threads)]
        for _ in as_completed(futures):
            pass

    # 7) Merge & write out
    store.update(results)
    with open(PERSONS_JSON, 'w', encoding='utf-8') as f:
        json.dump(store, f, indent=2, ensure_ascii=False)
    with open(FAILS_JSON, 'w', encoding='utf-8') as f:
        json.dump(new_fails, f, indent=2)

    print(f"✅ Completed: {len(store)} individual records written to {PERSONS_JSON}")
    if new_fails:
        print(f"⚠️  Warning: {len(new_fails)} IRNs failed to fetch")

if __name__ == '__main__':
    main()
