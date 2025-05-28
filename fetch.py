# fetch.py

import os
import time
import requests
import logging
from rate_limiter import enforce_rate_limit

API_URL     = "https://api.company-information.service.gov.uk/advanced-search/companies"
FETCH_SIZE  = 100
RETRIES     = 3
logger      = logging.getLogger(__name__)

def get_api_key() -> str:
    key = os.getenv("CH_API_KEY")
    if not key:
        logger.error("CH_API_KEY not set")
        raise RuntimeError("CH_API_KEY unset")
    return key

def fetch_page(date: str, start_index: int) -> dict:
    """
    Fetch one page of companies for `date` at offset `start_index`.
    Always sends `size` + `start_index`. On persistent 5xx, logs & returns {}.
    """
    enforce_rate_limit()

    params = {
        "incorporated_from": date,
        "incorporated_to":   date,
        "size":              FETCH_SIZE,
        "start_index":       start_index,
    }

    for attempt in range(1, RETRIES + 1):
        resp = requests.get(API_URL, auth=(get_api_key(), ""), params=params, timeout=10)
        if resp.status_code >= 500:
            if attempt < RETRIES:
                backoff = 2 ** (attempt - 1)
                logger.warning(f"Server error {resp.status_code} on {date}@{start_index}, retrying in {backoff}s…")
                time.sleep(backoff)
                continue
            else:
                snippet = resp.text.replace("\n", " ")[:200]
                logger.error(
                    f"Persistent 5xx ({resp.status_code}) on {date}@{start_index} "
                    f"after {RETRIES} attempts; response snippet: {snippet!r}"
                )
                return {}
        try:
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            logger.error(f"HTTP error {e} fetching {date}@{start_index}: {resp.text[:200]!r}")
            raise

    # Fallback should never be reached
    return {}

def fetch_companies_for_date(date: str) -> list:
    """Fetch all pages of results for a single ISO date string."""
    logger.info(f"Fetching page 0 for {date}")
    first = fetch_page(date, 0)
    total = first.get("hits", first.get("total_results", 0)) or 0
    logger.info(f" → {total} total companies on {date}")
    items = first.get("items", [])

    pages = (total + FETCH_SIZE - 1) // FETCH_SIZE
    for page in range(1, pages):
        offset = page * FETCH_SIZE
        logger.info(f"Fetching page {page} (offset {offset})")
        batch = fetch_page(date, offset)
        items.extend(batch.get("items", []))

    return items
