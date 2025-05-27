# fund_tracker.py

import os
import sys
import json
import time
import math
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta

# Constants for retry logic
INTERNAL_FETCH_RETRIES = 3    # HTTP retries per run
MAX_RUN_RETRIES       = 5     # Scheduled-run retries before “dead”

# Paths
FAILED_FILE = os.path.join("docs", "assets", "data", "failed_pages.json")
MASTER_CSV  = os.path.join("docs", "assets", "data", "master_companies.csv")
RELEVANT_CSV= os.path.join("docs", "assets", "data", "relevant_companies.csv")

# Setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

MAX_PER_PAGE = 100

def fetch_page(date_str: str, start_index: int, api_key: str) -> dict:
    url = (
        f"https://api.company-information.service.gov.uk/advanced-search/companies"
        f"?incorporated_from={date_str}&incorporated_to={date_str}"
        f"&size={MAX_PER_PAGE}&start_index={start_index}"
    )
    backoff = 1
    for attempt in range(1, INTERNAL_FETCH_RETRIES + 1):
        r = requests.get(url, auth=(api_key, ""))
        if r.status_code >= 500 and attempt < INTERNAL_FETCH_RETRIES:
            logger.warning(
                f"5xx on {date_str}@{start_index}, retry {attempt}/{INTERNAL_FETCH_RETRIES}"
            )
            time.sleep(backoff * (2 ** (attempt - 1)))
            continue
        try:
            r.raise_for_status()
            return r.json()
        except requests.HTTPError:
            # On final 5xx or any 4xx, bubble up
            raise
    # Exhausted all internal retries
    raise RuntimeError(f"Failed to fetch {date_str}@{start_index} after {INTERNAL_FETCH_RETRIES} tries")

def date_range(start: datetime, end: datetime):
    curr = start
    while curr <= end:
        yield curr.strftime("%Y-%m-%d")
        curr += timedelta(days=1)

def run_for_range(start_date_str: str, end_date_str: str):
    api_key = os.getenv("CH_API_KEY")
    if not api_key:
        logger.error("CH_API_KEY not set")
        sys.exit(1)

    # Parse dates
    sd = datetime.today() if start_date_str == "today" else datetime.fromisoformat(start_date_str)
    ed = datetime.today() if end_date_str   == "today" else datetime.fromisoformat(end_date_str)

    # Load previous failures
    if os.path.exists(FAILED_FILE):
        with open(FAILED_FILE) as f:
            old = json.load(f)
        failed_pages = { (r["date"], r["offset"]): r["count"] for r in old }
    else:
        failed_pages = {}
    new_failed = {}

    all_records = []

    # 1) Fetch fresh pages in date range
    for ds in date_range(sd, ed):
        # Fetch first page to get total_results
        try:
            first = fetch_page(ds, 0, api_key)
        except Exception as e:
            logger.error(f"First page failed for {ds}: {e}")
            failed_pages[(ds, 0)] = failed_pages.get((ds, 0), 0) + 1
            continue

        total = first.get("total_results", 0)
        pages = math.ceil(total / MAX_PER_PAGE)
        all_records.extend(first.get("items", []))

        for i in range(1, pages):
            offset = i * MAX_PER_PAGE
            key = (ds, offset)
            try:
                batch = fetch_page(ds, offset, api_key)
                all_records.extend(batch.get("items", []))
            except Exception as e:
                cnt = failed_pages.get(key, 0) + 1
                if cnt < MAX_RUN_RETRIES:
                    new_failed[key] = cnt
                else:
                    logger.error(f"Dead page {key} after {cnt} runs")

    # 2) Re-attempt old failures once more
    for (ds, offset), cnt in failed_pages.items():
        if cnt >= MAX_RUN_RETRIES:
            continue
        key = (ds, offset)
        try:
            batch = fetch_page(ds, offset, api_key)
            all_records.extend(batch.get("items", []))
        except Exception as e:
            new_cnt = cnt + 1
            if new_cnt < MAX_RUN_RETRIES:
                new_failed[key] = new_cnt
            else:
                logger.error(f"Dead on retry: {key}")

    # 3) Persist updated failure list
    os.makedirs(os.path.dirname(FAILED_FILE), exist_ok=True)
    out = [{"date": d, "offset": o, "count": c} for (d,o),c in new_failed.items()]
    with open(FAILED_FILE, "w") as f:
        json.dump(out, f, indent=2)

    # 4) Enrich & write CSV/XLSX (your existing logic)
    df = pd.DataFrame(all_records)
    # … classification, SIC enrichment, dedupe, etc. …
    df.to_csv(MASTER_CSV, index=False)
    # … filter to relevant …
    df_relevant = df[ df['Category'] != 'Other' | df['SIC Description'].notna() ]
    df_relevant.to_csv(RELEVANT_CSV, index=False)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--start_date", required=True)
    p.add_argument("--end_date", required=True)
    args = p.parse_args()
    run_for_range(args.start_date, args.end_date)
