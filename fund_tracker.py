#!/usr/bin/env python3
import os
import sys
import json
import time
import math
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta

# ─── Configuration ───────────────────────────────────────────────────────────────

# How many HTTP retry attempts per call (with exponential backoff)
INTERNAL_FETCH_RETRIES = 3

# How many separate scheduled runs to retry a page before giving up
MAX_RUN_RETRIES = 5

# Max items per page for Companies House
MAX_PER_PAGE = 100

# ─── Paths ───────────────────────────────────────────────────────────────────────

DATA_DIR     = os.path.join("docs", "assets", "data")
FAILED_FILE  = os.path.join(DATA_DIR, "failed_pages.json")
MASTER_CSV   = os.path.join(DATA_DIR, "master_companies.csv")
RELEVANT_CSV = os.path.join(DATA_DIR, "relevant_companies.csv")

# ─── Logger Setup ────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─── Fetch with Retry ────────────────────────────────────────────────────────────

def fetch_page(date_str: str, start_index: int, api_key: str) -> dict:
    """
    Fetch one page of companies for a given date and offset.
    Retries up to INTERNAL_FETCH_RETRIES times on HTTP 5xx, with exponential backoff.
    Raises on 4xx or final 5xx.
    """
    url = (
        f"https://api.company-information.service.gov.uk/advanced-search/companies"
        f"?incorporated_from={date_str}&incorporated_to={date_str}"
        f"&size={MAX_PER_PAGE}&start_index={start_index}"
    )

    backoff_base = 1
    for attempt in range(1, INTERNAL_FETCH_RETRIES + 1):
        resp = requests.get(url, auth=(api_key, ""))
        status = resp.status_code

        # If server error and more retries remain, back off and retry
        if 500 <= status < 600 and attempt < INTERNAL_FETCH_RETRIES:
            wait = backoff_base * (2 ** (attempt - 1))
            logger.warning(f"Server error {status} on {date_str}@{start_index}, "
                           f"retry {attempt}/{INTERNAL_FETCH_RETRIES} after {wait}s")
            time.sleep(wait)
            continue

        try:
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            logger.error(f"HTTP error {status} on {url}: {e}")
            raise

    # If loop completes without returning, treat as fatal
    raise RuntimeError(f"Exhausted retries for {date_str}@{start_index}")

# ─── Date Utilities ───────────────────────────────────────────────────────────────

def date_range(start: datetime, end: datetime):
    curr = start
    while curr <= end:
        yield curr.strftime("%Y-%m-%d")
        curr += timedelta(days=1)

# ─── Main Run Logic ───────────────────────────────────────────────────────────────

def run_for_range(start_date_str: str, end_date_str: str):
    api_key = os.getenv("CH_API_KEY")
    if not api_key:
        logger.error("CH_API_KEY environment variable is not set")
        sys.exit(1)

    # Parse dates (treat empty or "today" as now)
    if not start_date_str or start_date_str.lower() == "today":
        sd = datetime.utcnow()
    else:
        sd = datetime.fromisoformat(start_date_str)
    if not end_date_str or end_date_str.lower() == "today":
        ed = datetime.utcnow()
    else:
        ed = datetime.fromisoformat(end_date_str)

    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    # Load previously failed pages
    if os.path.exists(FAILED_FILE):
        with open(FAILED_FILE) as f:
            prev = json.load(f)
        failed_pages = { (r["date"], r["offset"]): r["count"] for r in prev }
    else:
        failed_pages = {}
    new_failed = {}
    all_records = []

    # 1) Fetch fresh pages in the configured date range
    for ds in date_range(sd, ed):
        # First page to obtain total_results
        try:
            first = fetch_page(ds, 0, api_key)
        except Exception as e:
            logger.error(f"Failed initial page for {ds}: {e}")
            cnt = failed_pages.get((ds, 0), 0) + 1
            if cnt < MAX_RUN_RETRIES:
                new_failed[(ds, 0)] = cnt
            else:
                logger.error(f"Dead page {(ds,0)} after {cnt} runs")
            continue

        total = first.get("total_results", 0) or 0
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

    # 2) Retry previously failed pages one more time this run
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

    # 3) Persist updated failures list
    out = [{"date": d, "offset": o, "count": c} for (d,o), c in new_failed.items()]
    with open(FAILED_FILE, "w") as f:
        json.dump(out, f, indent=2)

    # 4) Enrich and write CSVs
    df = pd.DataFrame(all_records)
    # … classification, SIC enrichment, dedupe, etc. …
    df.to_csv(MASTER_CSV, index=False)
    relevant = df[(df["Category"] != "Other") | (df["SIC Description"].notna())]
    relevant.to_csv(RELEVANT_CSV, index=False)

    logger.info(f"Wrote {len(all_records)} total records; {len(relevant)} relevant")

# ─── Entry Point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", required=True)
    parser.add_argument("--end_date",   required=True)
    args = parser.parse_args()

    sd_in = args.start_date or "today"
    ed_in = args.end_date   or "today"
    run_for_range(sd_in, ed_in)
