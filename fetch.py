import os
import time
import requests
from datetime import datetime
from logger import log
from enrich import classify, enrich_sic

# Advanced-Search endpoint
CH_API_URL    = "https://api.company-information.service.gov.uk/advanced-search/companies"
FETCH_SIZE    = 100     # 100 per page
RETRY_COUNT   = 3
RETRY_DELAY   = 5       # seconds

def get_api_key() -> str:
    key = os.getenv("CH_API_KEY")
    if not key:
        log.error("CH_API_KEY not set")
        raise RuntimeError("CH_API_KEY unset")
    return key

def fetch_companies_on(date_str: str) -> list[dict]:
    """
    Call Advanced-Search; page through all results, retrying each page on error.
    Returns a list of *record dicts* ready for appending to master.
    """
    auth = (get_api_key(), "")
    all_items = []
    start_index = 0
    size = FETCH_SIZE

    # 1) page through JSON items
    while True:
        params = {
            'incorporated_from': date_str,
            'incorporated_to':   date_str,
            'size':              size,
            'start_index':       start_index,
        }
        page_items = []
        for attempt in range(1, RETRY_COUNT + 1):
            try:
                resp = requests.get(CH_API_URL, auth=auth, params=params, timeout=10)
                if resp.status_code == 200:
                    page_items = resp.json().get('items', [])
                    break
                else:
                    log.warning(f'Non-200 ({resp.status_code}) on {date_str}@{start_index} attempt {attempt}')
            except Exception as e:
                log.warning(f'Error on {date_str}@{start_index} attempt {attempt}: {e}')
            time.sleep(RETRY_DELAY)
        else:
            log.error(f'Failed to fetch page at index {start_index} for {date_str}')
            break

        if not page_items:
            # nothing left—or persistent error
            break

        all_items.extend(page_items)
        if len(page_items) < size:
            # last page
            break
        start_index += size

    # 2) transform JSON items into record dicts
    now = datetime.utcnow()
    recs = []
    for c in all_items:
        name = c.get('title') or c.get('company_name', '')
        raw_codes = c.get('sic_codes', []) or []
        joined_codes = ", ".join(raw_codes)
        sic_desc, sic_use = enrich_sic(raw_codes)

        # Determine Category: if classify(name) is "Other" but sic_desc is non-empty, label as "SIC"
        base_cat = classify(name)
        if base_cat == "Other" and sic_desc:
            category = "SIC"
        else:
            category = base_cat

        recs.append({
            'CompanyNumber':     c.get('company_number', ''),
            'CompanyName':       name,
            'IncorporationDate': c.get('date_of_creation', ''),
            'Status':            c.get('company_status', ''),
            'Source':            c.get('source', ''),
            'DateDownloaded':    now.date().isoformat(),
            'TimeDiscovered':    now.isoformat(),
            'SIC Codes':         joined_codes,
            'Category':          category,
            'SIC Description':   sic_desc,
            'Typical Use Case':  sic_use,
        })
    return recs
