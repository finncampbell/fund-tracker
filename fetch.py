import os
import time
import requests
from datetime import datetime
from logger import log         # still used for errors/warnings to the log file
from enrich import classify, enrich_sic

# Advanced-Search endpoint
CH_API_URL  = "https://api.company-information.service.gov.uk/advanced-search/companies"
FETCH_SIZE  = 100
RETRY_COUNT = 3
RETRY_DELAY = 5  # seconds

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

    # ─── SANITY CHECK: one-shot “size=5000” ─────────────────────────────────────
    try:
        resp_big = requests.get(
            CH_API_URL,
            auth=auth,
            params={
                'incorporated_from': date_str,
                'incorporated_to':   date_str,
                'size':              5000,
                'start_index':       0,
            },
            timeout=10
        )
        if resp_big.status_code == 200:
            big_items = resp_big.json().get('items', [])
            print(f"[SANITY] One-shot (size=5000) returned {len(big_items)} items for {date_str}")
        else:
            print(f"[SANITY] One-shot returned status {resp_big.status_code} for {date_str}")
    except Exception as e:
        print(f"[SANITY] One-shot exception for {date_str}: {e}")

    page_number = 0
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
                    log.warning(f"[PAGINATION] Non-200 ({resp.status_code}) on {date_str}@{start_index} (attempt {attempt})")
            except Exception as e:
                log.warning(f"[PAGINATION] Exception on {date_str}@{start_index} attempt {attempt}: {e}")
            print(f"[PAGINATION] sleeping {RETRY_DELAY}s before retrying page {start_index}")
            time.sleep(RETRY_DELAY)
        else:
            log.error(f"[PAGINATION] FAILED to fetch any data at {date_str}@{start_index}; aborting pagination")
            break

        print(f"[PAGINATION] Page {page_number}, start_index={start_index}, got {len(page_items)} items")
        page_number += 1

        if not page_items:
            # no more results (or persistent error)
            break

        all_items.extend(page_items)

        if len(page_items) < size:
            print(f"[PAGINATION] Last page likely hit (only {len(page_items)} < {size})")
            break

        start_index += size

    print(f"[PAGINATION] Finished for {date_str}: fetched {len(all_items)} total items over {page_number} pages")

    # ─── Transform JSON items into “master” record dicts ─────────────────────────
    now = datetime.utcnow()
    recs = []
    for c in all_items:
        name = c.get('title') or c.get('company_name', '')
        raw_codes = c.get('sic_codes', []) or []
        joined_codes = ", ".join(raw_codes)
        sic_desc, sic_use = enrich_sic(raw_codes)

        base_cat = classify(name)
        if sic_desc:
            if base_cat == "Other":
                category = "SIC"
            else:
                category = f"{base_cat}, SIC"
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
