#!/usr/bin/env python3
"""
scripts/fetch_firm_individuals.py

Fetch all FCA firm individuals in parallel threads (work-stealing),
with live progress counter and shared rate limiting.

Now reads the master FRN list from docs/fca-dashboard/data/all_frns_with_names.json,
so it always uses the consolidated seed list in your GitHub Pages source.

Usage:
  python3 fca-dashboard/scripts/fetch_firm_individuals.py [--threads N] [--limit M] [--output PATH]
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
    description='Fetch FCA firm individuals in parallel with live progress'
)
parser.add_argument(
    '--threads', type=int, default=5,
    help='Number of parallel threads to use'
)
parser.add_argument(
    '--limit', type=int, default=None,
    help='Optional cap on total FRNs to process (for quick tests)'
)
parser.add_argument(
    '--output', type=str,
    default=os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        '../../docs/fca-dashboard/data/fca_individuals_by_firm.json'
    )),
    help='Path where the final merged JSON will be written'
)
args = parser.parse_args()

# ─── Paths & Data ───────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(__file__)
# Point at the docs copy of the FRN seed list
FRN_FILE = os.path.abspath(os.path.join(
    SCRIPT_DIR,
    '../../docs/fca-dashboard/data/all_frns_with_names.json'
))

# ─── FCA API setup ───────────────────────────────────────────────────────────
API_EMAIL = os.getenv('FCA_API_EMAIL')
API_KEY   = os.getenv('FCA_API_KEY')
if not API_EMAIL or not API_KEY:
    raise RuntimeError('FCA_API_EMAIL and FCA_API_KEY must be set in the environment')

BASE_URL = 'https://register.fca.org.uk/services/V0.1'
HEADERS = {
    'Accept':       'application/json',
    'X-AUTH-EMAIL': API_EMAIL,
    'X-AUTH-KEY':   API_KEY,
}

# Shared RateLimiter reads RL_MAX_CALLS and RL_WINDOW_S from env
limiter = RateLimiter()

# ─── Worker & Progress Setup ────────────────────────────────────────────────
q = Queue()
lock = Lock()
processed = 0

def fetch_json(url):
    """Rate-limited GET returning parsed JSON."""
    limiter.wait()
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()

def worker(results, total):
    """
    Thread worker: pulls FRNs from the queue, pages through their /Individuals endpoint,
    normalizes the records, stores in `results`, and prints a live counter.
    """
    global processed
    while True:
        try:
            frn = q.get_nowait()
        except Empty:
            return

        # 1) Page through this firm's individuals
        url = f"{BASE_URL}/Firm/{frn}/Individuals"
        all_recs = []
        while url:
            pkg = fetch_json(url)
            # pkg['Data'] may be None → coerce to empty list
            all_recs.extend(pkg.get('Data') or [])
            ri = pkg.get('ResultInfo') or {}
            url = ri.get('Next')

        # 2) Normalize each record to essential fields
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

        # 3) Update shared progress counter
        with lock:
            processed += 1
            print(f"▶️  Processed {processed}/{total} FRNs ({total-processed} remaining)")

        q.task_done()

def main():
    # ─── Load and optionally trim the FRN list ────────────────────────────────
    with open(FRN_FILE, 'r', encoding='utf-8') as f:
        frn_entries = json.load(f)
    frns = [entry['frn'] for entry in frn_entries]

    if args.limit:
        # Test mode: only process the first M FRNs
        frns = frns[:args.limit]

    total = len(frns)
    print(f"🔍 Starting threaded fetch for {total} FRNs using {args.threads} threads")

    # ─── Enqueue all work ─────────────────────────────────────────────────────
    for frn in frns:
        q.put(frn)

    # Shared results dict
    results = {}

    # ─── Execute threads for dynamic work-stealing ────────────────────────────
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        # Submit one worker task per thread
        futures = [executor.submit(worker, results, total)
                   for _ in range(args.threads)]
        # Wait for all to complete
        for _ in as_completed(futures):
            pass

    # ─── Write final JSON for the UI ──────────────────────────────────────────
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"✅ All done. Wrote {len(results)} FRN entries to {args.output}")

if __name__ == '__main__':
    main()
