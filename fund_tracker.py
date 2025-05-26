import logging
import pandas as pd
import re
import requests
from datetime import datetime, timedelta

# --- Filtering patterns and lookups ---

# Name-based ("Fund Entities") patterns for high-precision fund entity detection
CLASS_PATTERNS = [
    (re.compile(r'\bL[\.\-\s]?L[\.\-\s]?P\b', re.IGNORECASE), 'LLP'),
    (re.compile(r'\bL[\.\-\s]?P\b',           re.IGNORECASE), 'LP'),
    (re.compile(r'\bG[\.\-\s]?P\b',           re.IGNORECASE), 'GP'),
    (re.compile(r'\bFund\b',                  re.IGNORECASE), 'Fund'),
    # ... other business-service keywords as desired ...
]

def classify(name):
    """
    Assign a Category to a company name based on CLASS_PATTERNS.
    Returns the label of the first matching pattern, else 'Other'.
    """
    for pat, label in CLASS_PATTERNS:
        if pat.search(name or ''):
            return label
    return 'Other'

# Fill this with all your relevant codes:
SIC_LOOKUP = {
    "64205": ("Financial services holding companies", "Fund vehicles, Holding companies"),
    "64209": ("Other holding companies n.e.c.", "SPVs, Holding companies"),
    "64301": ("Investment trusts", "Fund vehicles"),
    "64302": ("Unit trusts", "Fund vehicles"),
    "64303": ("Venture and development capital companies", "Venture funds"),
    "64304": ("Open‐ended investment companies", "OEICs"),
    "64305": ("Property unit trusts", "Fund vehicles"),
    "64306": ("Real estate investment trusts", "REITs"),
    "64921": ("Credit granting by non-deposit-taking finance houses", "Debt vehicles"),
    "64922": ("Mortgage finance companies", "Debt vehicles"),
    "64929": ("Other credit granting n.e.c.", "Debt vehicles"),
    "64991": ("Security dealing on own account", "Fund vehicles"),
    "64999": ("Financial intermediation not elsewhere classified", "Holding companies, financial vehicles"),
    "66300": ("Fund management activities", "Fund managers, management companies"),
    "70100": ("Activities of head offices", "Holding companies"),
    "70221": ("Financial management of companies and enterprises", "Service providers"),
    # etc.
}

def enrich_sic(codes):
    """
    For a list of SIC codes, join them, and map to descriptions and use cases.
    Returns (joined, description string, use case string).
    """
    joined = ",".join(codes)
    descs, uses = [], []
    for code in codes:
        if code in SIC_LOOKUP:
            d, u = SIC_LOOKUP[code]
            descs.append(d)
            uses.append(u)
    return joined, "; ".join(descs), "; ".join(uses)

def process_companies(raw_companies):
    """
    Process the raw company records, classifying and enriching with SIC info.
    Returns a DataFrame with all master records.
    """
    records = []
    for rec in raw_companies:
        name = rec.get('company_name', '')
        category = classify(name)
        codes = rec.get('sic_codes', [])
        if not isinstance(codes, list):
            codes = []
        joined, descs, uses = enrich_sic(codes)
        records.append({
            **rec,
            'Category': category,
            'SIC Codes': joined,
            'SIC Description': descs,
            'Typical Use Case': uses,
        })
    return pd.DataFrame(records)

def build_relevant_slice(df_master):
    """
    Build the relevant companies DataFrame: those with fund-entity names or relevant SIC codes.
    """
    mask_cat = df_master['Category'] != 'Other'
    mask_sic = df_master['SIC Codes'].str.split(',').apply(
        lambda codes: any(c in SIC_LOOKUP for c in codes) if isinstance(codes, list) else False
    )
    df_rel = df_master[mask_cat | mask_sic]
    return df_rel

def fetch_all_companies_for_date(date, api_key):
    """Fetch all companies incorporated on a given date, handling pagination."""
    all_records = []
    start_index = 0
    size = 100
    while True:
        url = "https://api.company-information.service.gov.uk/advanced-search/companies"
        params = {
            "incorporated_from": date,
            "incorporated_to": date,
            "size": size,
            "start_index": start_index,
        }
        r = requests.get(url, params=params, auth=(api_key, ''))
        r.raise_for_status()
        data = r.json()
        items = data.get("items", [])
        all_records.extend(items)
        total = data.get("total_results", 0)
        if len(all_records) >= total or not items:
            break
        start_index += size
    return all_records

def daterange(start_date, end_date):
    """Yield each date as a string in YYYY-MM-DD from start_date to end_date inclusive."""
    curr = start_date
    while curr <= end_date:
        yield curr.strftime('%Y-%m-%d')
        curr += timedelta(days=1)

def run_for_range(start_date, end_date, api_key):
    """Fetches, processes, and saves master and relevant company slices for the date range."""
    all_raw = []
    for day in daterange(start_date, end_date):
        logging.info(f"Fetching companies for {day}")
        day_records = fetch_all_companies_for_date(day, api_key)
        all_raw.extend(day_records)
    logging.info(f"Fetched {len(all_raw)} total company records for range.")

    df_master = process_companies(all_raw)
    df_rel = build_relevant_slice(df_master)

    df_master.to_csv('master_companies.csv', index=False)
    df_rel.to_csv('relevant_companies.csv', index=False)
    logging.info(f"Wrote master ({len(df_master)}) and relevant ({len(df_rel)}) records.")

def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--api-key", required=True, help="Companies House API key")
    return parser.parse_args()

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    start = datetime.strptime(args.start_date, '%Y-%m-%d')
    end = datetime.strptime(args.end_date, '%Y-%m-%d')
    run_for_range(start, end, args.api_key)

if __name__ == "__main__":
    main()
