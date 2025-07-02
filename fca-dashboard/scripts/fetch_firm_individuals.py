#!/usr/bin/env python3
"""
scripts/fetch_firm_individuals.py

Fetch all FCA firm individuals in parallel threads (work-stealing),
with live progress counter and shared rate limiting.

Supports:
  • --threads: number of worker threads (default 5)
  • --limit:   optional cap on total FRNs to process (for testing)
  • --output:  path to write the final JSON (default: docs/.../fca_individuals_by_firm.json)
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
    help='Optional cap on total FRNs to process'
)
parser.add_argument(
    '--output', type=str,
    default=os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        '../../docs/fca-dashboard/data/fca_individuals_by_firm.json'
    )),
    help='Path to write the final merged JSON'
)
args = parser.parse_args()

# ─── Paths & Data ───────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(__file__)
DATA_DIR   = os.path.abspath(os.path.join(SCRIPT_DIR, '../data'))
FRN_FILE   = os.path.join(DATA_DIR, 'all_frns_with_names.json')

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

# Shared rate limiter (reads RL_MAX_CALLS and RL_WINDOW_S, default 50/10)
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
    """Thread worker: pulls FRNs from the queue, fetches, stores, updates counter."""
    global processed
    while True:
        try:
            frn = q.get_nowait()
        except Empty:
            return

        # Page through this FRN’s individuals
        url = f"{BASE_URL}/Firm/{frn}/Individuals"
        all_recs = []
        while url:
            pkg = fetch_json(url)
            all_recs.extend(pkg.get('Data') or [])
            ri = pkg.get('ResultInfo') or {}
            url = ri.get('Next')

        # Normalize records
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

        # Update and print live progress
        with lock:
            processed += 1
            print(f"▶️  Processed {processed}/{total} FRNs ({total-processed} remaining)")

        q.task_done()

def main():
    # Load FRN list
    with open(FRN_FILE, 'r', encoding='utf-8') as f:
        frn_entries = json.load(f)
    frns = [entry['frn'] for entry in frn_entries]

    # Apply test limit if provided
    if args.limit:
        frns = frns[:args.limit]

    total = len(frns)

    # Populate queue
    for frn in frns:
        q.put(frn)

    # Shared dict for results
    results = {}

    # ThreadPool with dynamic work-stealing
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = [executor.submit(worker, results, total)
                   for _ in range(args.threads)]
        # Wait for all threads to finish
        for _ in as_completed(futures):
            pass

    # Write final merged JSON
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"✅ All done. Wrote {len(results)} FRN entries to {args.output}")

if __name__ == '__main__':
    main()
