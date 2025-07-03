#!/usr/bin/env python3
"""
scripts/fetch_firm_individuals.py

Threaded fetch of FCA firm-individual mappings, with flexible modes:
  --threads N          number of worker threads
  --limit M            cap on total FRNs (for quick tests)
  --only-missing       only FRNs not already in the UI JSON
  --only-blank         only FRNs whose last-run results were empty
  --retry-failed       only FRNs that errored in the previous run
  --skip-large         skip FRNs with existing-count > threshold
  --only-large         only FRNs with existing-count > threshold
  --large-threshold X  threshold for “large” firms (default 50)
  --dry-run            don’t call the API, just list FRNs
  --output PATH        where to write final merged JSON
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
parser = argparse.ArgumentParser()
parser.add_argument('--threads',         type=int,    default=5)
parser.add_argument('--limit',           type=int,    default=None)
parser.add_argument('--only-missing',    action='store_true')
parser.add_argument('--only-blank',      action='store_true')
parser.add_argument('--retry-failed',    action='store_true')
parser.add_argument('--skip-large',      action='store_true')
parser.add_argument('--only-large',      action='store_true')
parser.add_argument('--large-threshold', type=int,    default=50)
parser.add_argument('--dry-run',         action='store_true')
parser.add_argument(
    '--output',
    type=str,
    default=os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        '../../docs/fca-dashboard/data/fca_individuals_by_firm.json'
    ))
)
args = parser.parse_args()

# ─── Paths & Data ───────────────────────────────────────────────────────────
SCRIPT_DIR    = os.path.dirname(__file__)
SEED_FILE     = os.path.abspath(os.path.join(
    SCRIPT_DIR,
    '../../docs/fca-dashboard/data/all_frns_with_names.json'
))
UI_JSON       = args.output
FAILS_JSON    = os.path.abspath(os.path.join(
    SCRIPT_DIR,
    '../../docs/fca-dashboard/data/fca_individuals_fails.json'
))

# ─── Load seed FRNs ──────────────────────────────────────────────────────────
with open(SEED_FILE, 'r', encoding='utf-8') as f:
    frns = [e['frn'] for e in json.load(f)]

# ─── Optionally load existing results & failures ───────────────────────────
existing = {}
if os.path.exists(UI_JSON):
    existing = json.load(open(UI_JSON, 'r', encoding='utf-8'))

failed_last = []
if args.retry_failed and os.path.exists(FAILS_JSON):
    failed_last = json.load(open(FAILS_JSON, 'r', encoding='utf-8'))

# ─── Apply mode filters (skip or select subsets) ────────────────────────────
if args.only_missing:
    frns = [f for f in frns if str(f) not in existing]
elif args.only_blank:
    frns = [f for f in frns if existing.get(str(f), []) == []]
elif args.retry_failed:
    frns = [f for f in failed_last]

if args.skip_large:
    frns = [f for f in frns
            if len(existing.get(str(f), [])) <= args.large_threshold]
elif args.only_large:
    frns = [f for f in frns
            if len(existing.get(str(f), []))  > args.large_threshold]

# ─── Test‐mode cap ────────────────────────────────────────────────────────────
if args.limit:
    frns = frns[:args.limit]

total = len(frns)
print(f"🔍 Preparing to process {total} FRNs "
      f"(threads={args.threads}, dry_run={args.dry_run})")

# ─── Dry‐run: list & exit ────────────────────────────────────────────────────
if args.dry_run:
    for f in frns:
        print(f"➡️  Would fetch FRN {f}")
    print("✅ Dry run complete; no HTTP calls made.")
    exit(0)

# ─── Set up threading & rate‐limiting ────────────────────────────────────────
limiter = RateLimiter()  # uses RL_MAX_CALLS / RL_WINDOW_S from env
q = Queue()
lock = Lock()
processed = 0
results = {}
fails = []

# ─── Populate the work queue ────────────────────────────────────────────────
for frn in frns:
    q.put(frn)

def fetch_json(url):
    limiter.wait()
    r = requests.get(url, headers={
        'Accept':       'application/json',
        'X-AUTH-EMAIL': os.getenv('FCA_API_EMAIL'),
        'X-AUTH-KEY':   os.getenv('FCA_API_KEY'),
    }, timeout=10)
    r.raise_for_status()
    return r.json()

def worker(total):
    """Thread worker: fetches one FRN at a time, handles errors & no-data."""
    global processed
    while True:
        try:
            frn = q.get_nowait()
        except Empty:
            return

        try:
            url = f"{BASE_URL}/Firm/{frn}/Individuals"
            all_recs = []
            while url:
                pkg = fetch_json(url)
                all_recs.extend(pkg.get('Data') or [])
                ri = pkg.get('ResultInfo') or {}
                url = ri.get('Next')

            normalized = [
                {
                    'IRN':    rec.get('IRN'),
                    'Name':   rec.get('Name'),
                    'Status': rec.get('Status'),
                    'URL':    rec.get('URL'),
                }
                for rec in all_recs
            ]
            results[frn] = normalized

            if not normalized:
                print(f"ℹ️  FRN {frn}: no individuals found")

        except Exception as e:
            results[frn] = []
            fails.append(frn)
            print(f"⚠️  Error fetching FRN {frn}: {e}")

        finally:
            with lock:
                processed += 1
                rem = total - processed
                print(f"▶️  Processed {processed}/{total} FRNs ({rem} remaining)")
            q.task_done()

# ─── Run the threads ────────────────────────────────────────────────────────
BASE_URL = 'https://register.fca.org.uk/services/V0.1'
with ThreadPoolExecutor(max_workers=args.threads) as pool:
    futures = [pool.submit(worker, total) for _ in range(args.threads)]
    for _ in as_completed(futures):
        pass

# ─── Merge & write the final JSON ───────────────────────────────────────────
merged = existing.copy()
merged.update(results)
os.makedirs(os.path.dirname(UI_JSON), exist_ok=True)
with open(UI_JSON, 'w', encoding='utf-8') as f:
    json.dump(merged, f, indent=2, ensure_ascii=False)
print(f"✅ All done. Wrote {len(merged)} FRN entries to {UI_JSON}")

# ─── Persist failures for retry_failed mode ────────────────────────────────
with open(FAILS_JSON, 'w', encoding='utf-8') as f:
    json.dump(fails, f, indent=2)
print(f"🔄 Recorded {len(fails)} failures to {FAILS_JSON}")
