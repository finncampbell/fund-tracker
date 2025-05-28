# fetch.py

import os
import time
import requests
import logging

# Configuration
API_URL     = "https://api.company-information.service.gov.uk/advanced-search/companies"
FETCH_SIZE  = 100   # items per request
RETRY_COUNT = 3
RETRY_DELAY = 5     # seconds

logger = logging.getLogger(__name__)

def get_api_key() -> str:
    key = os.getenv("CH_API_KEY")
    if not key:
        logger.error("CH_API_KEY not set")
        raise RuntimeError("CH_API_KEY unset")
    return key

def fetch_companies_for_date(date: str) -> list:
    """
    Fetch only the first page of companies incorporated on `date`.
    Retries up to RETRY_COUNT on non-200 or exceptions, then returns [].
    """
    auth = (get_api_key(), "")
    params = {
        "incorporated_from": date,
        "incorporated_to":   date,
        "size":              FETCH_SIZE,
    }

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(API_URL, auth=auth, params=params, timeout=10)
            if resp.status_code == 200:
                return resp.json().get("items", [])
            else:
                logger.warning(f"Non-200 ({resp.status_code}) on {date}, attempt {attempt}")
        except Exception as e:
            logger.warning(f"Error fetching {date}, attempt {attempt}: {e}")
        time.sleep(RETRY_DELAY)

    logger.error(f"Failed to fetch for {date} after {RETRY_COUNT} attempts")
    return []
