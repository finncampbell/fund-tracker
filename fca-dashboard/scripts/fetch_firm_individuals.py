#!/usr/bin/env python3
"""
scripts/fetch_firm_individuals.py

Threaded fetch of FCA firm-individual mappings, with flexible modes:
  --threads N          number of worker threads
  --limit M            cap on total FRNs for testing
  --only-missing       only FRNs not yet in the UI JSON
  --only-blank         only fetch FRNs whose previous result was empty
  --retry-failed       only retry FRNs that errored on the previous run
  --skip-large         skip FRNs with existing-count > threshold
  --only-large         only fetch FRNs with existing-count > threshold (forces single-thread)
  --large-threshold X  threshold for large firms in record count (default 50)
  --dry-run            dry run: show which FRNs would be processed
  --output PATH        where to write the final merged JSON
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
    description='Fetch FCA firm individuals in parallel with flexible modes'
)
parser.add_argument('--threads',         type=int,    default=5,
                    help='Number of worker threads')
parser.add_argument('--limit',           type=int,    default=None,
                    help='Optional cap on total FRNs for testing')
parser.add_argument('--only-missing',    action='store_true',
                    help='Only fetch FRNs not yet in the UI JSON')
parser.add_argument('--only-blank',      action='store_true',
                    help='Only fetch FRNs whose previous result was empty')
parser.add_argument('--retry-failed',    action='store_true',
                    help='Only retry FRNs that errored in the previous run')
parser.add_argument('--skip-large',      action='store_true',
                    help='Skip FRNs with existing-count > threshold')
parser.add_argument('--only-large',      action='store_true',
                    help='Only fetch FRNs with existing-count > threshold (forces single-thread)')
parser.add_argument('--large-threshold', type=int,    default=50,
                    help='Threshold for large firms in record count')
parser.add_argument('--dry-run',         action='store_true',
                    help='Dry run: show which FRNs would be processed')
parser.add_argument('--output',          type=str,
                    default=os.path.abspath(os.path.join(
                        os.path.dirname(__file__),
                        '../../docs/fca-dashboard/data/fca_individuals_by_firm.json'
                    )),
                    help='Path where the final merged JSON will be written')
args = parser.parse_args()

# ─── Force single-thread in only-large mode ─────────────────────────────────
if args.only_large:
    orig = args.threads
    args.threads = 1
    print(f"ℹ️  only_large mode: overriding threads from {orig} to 1 for max rate")

# ─── Paths & Data ───────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(__file__)
SEED_FILE  = os.path.abspath(os.path.join(
    SCRIPT_DIR,
    '../../docs/fca-dashboard/data/all_frns_with_names.json'
))
UI_JSON    = args.output
FAILS_JSON = os.path.abspath(os.path.join(
    SCRIPT_DIR,
    '../../docs/fca-dashboard/data/fca_individuals_fails.json'
))

# ─── Load seed list of FRNs ─────────────────────────────────────────────────
with open(SEED_FILE, 'r', encoding='utf-8') as f:
    frns = [entry['frn'] for entry in json.load(f)]

# ─── Load existing results & previous failures ──────────────────────────────
existing = {}
if os.path.exists(UI_JSON):
    with open(UI_JSON, 'r', encoding='utf-8') as f:
        existing = json.load(f)

failed_last = []
if args.retry_failed and os.path.exists(FAILS_JSON):
    with open(FAILS_JSON, 'r', encoding='utf-8') as f:
        failed_last = json.load(f)

# ─── Apply mode filters ─────────────────────────────────────────────────────
if args.only_missing:
    frns = [f for f in frns if str(f) not in existing]
elif args.only_blank:
    frns = [f for f in frns if existing.get(str(f), []) == []]
elif args.retry_failed:
    frns = failed_last

if args.skip_large:
    frns = [f for f in frns if len(existing.get(str(f), [])) <= args.large_threshold]
elif args.only_large:
    frns = [f for f in frns if len(existing.get(str(f), [])) > args.large_threshold]

# ─── Apply test limit if provided ───────────────────────────────────────────
if args.limit:
    frns = frns[:args.limit]

total = len(frns)
print(f"🔍 Preparing to process {total} FRNs "
      f"(threads={args.threads}, dry_run={args.dry_run})")

# ─── Dry-run: list FRNs & exit ──────────────────────────────────────────────
if args.dry_run:
    for f in frns:
        print(f"➡️  Would fetch FRN {f}")
    print("✅ Dry run complete; no API calls.")
    exit(0)

# ─── Set up rate limiter, queue, locks ─────────────────────────────────────
limiter = RateLimiter()  # uses RL_MAX_CALLS / RL_WINDOW_S from env
q = Queue()
lock = Lock()
processed = 0
results = {}
fails = []

# ─── Populate the work queue ───────────────────────────────────────────────
for frn in frns:
    q.put(frn)

# ─── HTTP helper ───────────────────────────────────────────────────────────
def fetch_json(url):
    limiter.wait()
    resp = requests.get(url, headers={
        'Accept':       'application/json',
        'X-AUTH-EMAIL': os.getenv('FCA_API_EMAIL'),
        'X-AUTH-KEY':   os.getenv('FCA_API_KEY'),
    }, timeout=10)
    resp.raise_for_status()
    return resp.json()

BASE_URL = 'https://register.fca.org.uk/services/V0.1'

# ─── Worker definition ─────────────────────────────────────────────────────
def worker(total):
    global processed
    while True:
        try:
            frn = q.get_nowait()
        except Empty:
            return

        try:
            # Peek missing/failed FRNs: one quick call to check total_count
            if str(frn) not in existing or existing.get(str(frn), []) == [] or frn in failed_last:
                peek_pkg = fetch_json(f"{BASE_URL}/Firm/{frn}/Individuals")
                ri = peek_pkg.get('ResultInfo') or {}
                total_count = int(ri.get('total_count') or 0)
                if total_count == 0:
                    results[frn] = []
                    print(f"ℹ️  FRN {frn}: peeked zero individuals, skipping deep fetch")
                    continue

            # 1) Page through this FRN's individuals
            url = f"{BASE_URL}/Firm/{frn}/Individuals"
            all_recs = []
            while url:
                pkg = fetch_json(url)
                all_recs.extend(pkg.get('Data') or [])
                ri = pkg.get('ResultInfo') or {}
                url = ri.get('Next')

            # 2) Normalize records
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

# ─── Execute threads ────────────────────────────────────────────────────────
with ThreadPoolExecutor(max_workers=args.threads) as executor:
    futures = [executor.submit(worker, total) for _ in range(args.threads)]
    for _ in as_completed(futures):
        pass

# ─── Merge & write final JSON ──────────────────────────────────────────────
merged = existing.copy()
merged.update(results)
os.makedirs(os.path.dirname(UI_JSON), exist_ok=True)
with open(UI_JSON, 'w', encoding='utf-8') as f:
    json.dump(merged, f, indent=2, ensure_ascii=False)
print(f"✅ All done. Wrote {len(merged)} FRN entries to {UI_JSON}")

# ─── Persist failures for next retry_failed run ────────────────────────────
with open(FAILS_JSON, 'w', encoding='utf-8') as f:
    json.dump(fails, f, indent=2)
print(f"🔄 Recorded {len(fails)} failures to {FAILS_JSON}")
