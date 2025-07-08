#!/usr/bin/env python3
"""
scripts/fetch_persons.py

Threaded fetch of core individual records (IRN, Name, Status, Controlled Functions URL)
with auto-checkpoint + self-dispatch before hitting GitHub's 6 h limit.

Modes:
  --threads N       Number of worker threads (default 5)
  --limit M         Cap on IRNs to process (for quick tests)
  --only-missing    Only fetch IRNs not yet in the store
  --retry-failed    Only retry IRNs that errored in the previous run
  --fresh           Ignore existing store and re-fetch all IRNs
  --dry-run         List IRNs without making any API calls
"""

import os
import sys
import time
import json
import argparse
import requests
from threading import Lock
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from rate_limiter import RateLimiter

# ─── PARAMETERS ───────────────────────────────────────────────────────────────
SIX_HOURS      = 6 * 3600
BUFFER_SECONDS = 300           # dispatch 5min before cutoff
START          = time.time()   # record when we began

# ─── DISPATCH GUARD ──────────────────────────────────────────────────────────
DISPATCHED = False

def dispatch_next_run():
    """
    POST a workflow_dispatch on this same workflow, forcing only_missing=true so
    the next run picks up only the IRNs we haven't fetched yet.
    """
    global DISPATCHED
    if DISPATCHED:
        return
    token    = os.getenv('GITHUB_TOKEN')
    repo     = os.getenv('GITHUB_REPOSITORY')
    # This file’s workflow filename
    workflow = os.getenv('GITHUB_WORKFLOW') or 'fetch-persons.yml'
    url      = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches"

    payload = {
        "ref": "main",
        "inputs": {
            "threads":      str(args.threads),
            "only_missing": "true",
            "retry_failed": str(args.retry_failed).lower(),
            "fresh":        str(args.fresh).lower(),
            "dry_run":      str(args.dry_run).lower(),
            "limit":        "",
            "no_push":      "true"
        }
    }
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=10)
        if not (200 <= r.status_code < 300):
            print(f"⚠️  Failed to dispatch follow-up run: {r.status_code} {r.text}")
        else:
            print("⏭️ Dispatched continuation workflow (only_missing=true).")
            DISPATCHED = True
    except Exception as e:
        print(f"⚠️  Exception dispatching continuation: {e}")

# ─── CLI ARGUMENTS ────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(
    description='Fetch and merge individual records in parallel with modes'
)
parser.add_argument('--threads',      type=int,    default=5,
                    help='Number of parallel worker threads')
parser.add_argument('--limit',        type=int,    default=None,
                    help='Optional cap on total IRNs to process (testing)')
parser.add_argument('--only-missing', action='store_true',
                    help='Only fetch IRNs not yet in the store')
parser.add_argument('--retry-failed', action='store_true',
                    help='Only retry IRNs that errored in the previous run')
parser.add_argument('--fresh',        action='store_true',
                    help='Ignore existing data and re-fetch all IRNs')
parser.add_argument('--dry-run',      action='store_true',
                    help='Dry run: list IRNs without API calls')
args = parser.parse_args()

# ─── PATHS ────────────────────────────────────────────────────────────────────
SCRIPT_DIR    = os.path.dirname(__file__)
DATA_DIR      = os.path.abspath(os.path.join(SCRIPT_DIR, '../../docs/fca-dashboard/data'))
IND_BY_FIRM   = os.path.join(DATA_DIR, 'fca_individuals_by_firm.json')
PERSONS_JSON  = os.path.join(DATA_DIR, 'fca_persons.json')
FAILS_JSON    = os.path.join(DATA_DIR, 'fca_persons_fails.json')

# ─── FCA API SETUP ─────────────────────────────────────────────────────────────
API_EMAIL = os.getenv('FCA_API_EMAIL')
API_KEY   = os.getenv('FCA_API_KEY')
if not API_EMAIL or not API_KEY:
    raise EnvironmentError('FCA_API_EMAIL and FCA_API_KEY must be set')

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
    except Exception as e:
        print(f"⚠️  Network error calling {url}: {e}")
        raise

    if resp.status_code == 429:
        print(f"⚠️  FCA API rate limit (429) at: {url}")
    elif resp.status_code >= 400:
        snippet = resp.text[:200].replace('\n',' ')
        print(f"⚠️  FCA API error {resp.status_code} at {url}: {snippet!r}")

    resp.raise_for_status()
    return resp.json()

def fetch_individual_record(irn: str):
    """Fetch and normalize one IRN record."""
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
    os.makedirs(DATA_DIR, exist_ok=True)

    # 1) Load the *up-to-date* merged IRN map from docs/
    if not os.path.exists(IND_BY_FIRM):
        print(f"❌ Missing {IND_BY_FIRM}: run fetch-firm-individuals first")
        return
    with open(IND_BY_FIRM,'r',encoding='utf-8') as f:
        firm_map = json.load(f)

    # 2) Flatten & dedupe IRNs
    all_irns = [rec['IRN']
                for entries in firm_map.values()
                for rec in entries
                if rec.get('IRN')]
    seen = set(); irns = [x for x in all_irns if x not in seen and not seen.add(x)]

    # 3) Load store unless fresh
    store = {}
    if os.path.exists(PERSONS_JSON) and not args.fresh:
        with open(PERSONS_JSON,'r',encoding='utf-8') as f:
            existing = json.load(f)
        if isinstance(existing,dict):
            store = existing

    # 4) Load previous failures if retrying
    fails = []
    if args.retry_failed and os.path.exists(FAILS_JSON):
        with open(FAILS_JSON,'r',encoding='utf-8') as f:
            fails = json.load(f)

    # 5) Apply modes
    if args.only_missing:
        irns = [i for i in irns if str(i) not in store]
    elif args.retry_failed:
        irns = fails

    # 6) Quick-test cap
    if args.limit:
        irns = irns[:args.limit]
        print(f"🔍 Test mode: fetching {len(irns)} IRNs")

    total = len(irns)
    print(f"🔍 Starting threaded fetch: {total} IRNs (threads={args.threads}, fresh={args.fresh})")

    # 7) Dry-run
    if args.dry_run:
        for i in irns:
            print(f"➡️ Would fetch IRN {i}")
        return

    # 8) Threaded fetch with checkpoint check
    q = Queue()
    for i in irns:
        q.put(i)
    lock = Lock()
    processed = 0
    results = {}
    new_fails = []

    def worker():
        nonlocal processed
        while True:
            # checkpoint: if we're within 5min of 6h, dispatch & exit
            if time.time() - START > SIX_HOURS - BUFFER_SECONDS:
                dispatch_next_run()
                sys.exit(0)

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
                    print(f"▶️ Processed {processed}/{total} ({rem} remaining)")
                q.task_done()

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        for _ in range(args.threads):
            executor.submit(worker)
        q.join()

    # 9) Merge & save
    store.update(results)
    with open(PERSONS_JSON,'w',encoding='utf-8') as f:
        json.dump(store,f,indent=2,ensure_ascii=False)
    with open(FAILS_JSON,'w',encoding='utf-8') as f:
        json.dump(new_fails,f,indent=2)

    print(f"✅ Completed: {len(store)} records in {PERSONS_JSON}")
    if new_fails:
        print(f"⚠️ {len(new_fails)} IRNs failed; see {FAILS_JSON}")

if __name__ == '__main__':
    main()
