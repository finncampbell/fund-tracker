import os
import sys
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging

# Constants
CH_API_URL = "https://api.company-information.service.gov.uk/advanced-search/companies"
OUTPUT_CSV = "assets/data/master_companies.csv"
OUTPUT_EXCEL = "master_companies.xlsx"
LOG_FILE = "fund_tracker.log"
RETRY_COUNT = 3

COLUMNS = [
    "Company Name",
    "Company Number",
    "Incorporation Date",
    "Status",
    "Source",
    "Date Downloaded",
    "Time Discovered"
]

# Set up logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

def fetch_companies_on(date_str, api_key):
    records = []
    params = {
        "incorporated_from": date_str,
        "incorporated_to": date_str,
        "size": 100
    }
    auth = (api_key, "")

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(CH_API_URL, auth=auth, params=params, timeout=10)
            if resp.status_code == 200:
                items = resp.json().get("items", [])
                logger.info(f"API returned {len(items)} items for {date_str}")
                now = datetime.utcnow()
                for item in items:
                    records.append({
                        "Company Name": item.get("company_name", ""),
                        "Company Number": item.get("company_number", ""),
                        "Incorporation Date": item.get("date_of_creation", ""),
                        "Status": item.get("company_status", ""),
                        "Source": "Companies House API",
                        "Date Downloaded": now.strftime("%Y-%m-%d"),
                        "Time Discovered": now.strftime("%H:%M:%S")
                    })
                break  # Successful fetch, break out of retry loop
            else:
                logger.warning(f"Non-200 ({resp.status_code}) on {date_str}, attempt {attempt}; Response: {resp.text}")
        except Exception as e:
            logger.warning(f"Error on {date_str}, attempt {attempt}: {e}")
        if attempt == RETRY_COUNT:
            logger.error(f"Failed to fetch companies for {date_str} after {RETRY_COUNT} attempts.")
    return records

def run_for_date_range(start_date: str, end_date: str, api_key: str):
    try:
        sd = datetime.strptime(start_date, "%Y-%m-%d")
        ed = datetime.strptime(end_date, "%Y-%m-%d")
    except Exception as e:
        logger.error(f"Invalid date format: {e}")
        sys.exit(1)

    if sd > ed:
        logger.error("start_date cannot be after end_date")
        sys.exit(1)

    logger.info(f"Starting run: {start_date} \u2192 {end_date}")
    records = []
    cur = sd
    while cur <= ed:
        ds = cur.strftime("%Y-%m-%d")
        logger.info(f"Fetching companies for {ds}")
        day_records = fetch_companies_on(ds, api_key)
        logger.info(f"Found {len(day_records)} companies for {ds}")
        records.extend(day_records)
        cur += timedelta(days=1)

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    # Always create DataFrame with correct columns
    df = pd.DataFrame(records, columns=COLUMNS)

    df.to_excel(OUTPUT_EXCEL, index=False)
    df.to_csv(OUTPUT_CSV, index=False)

    logger.info(f"Wrote {len(df)} records to {OUTPUT_EXCEL} & {OUTPUT_CSV}")

    if len(df) == 0:
        logger.warning("No companies found for the entire date range.")

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", required=True, help="Start date in YYYY-MM-DD or 'today'")
    parser.add_argument("--end_date", required=True, help="End date in YYYY-MM-DD or 'today'")
    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date

    # Handle 'today' keyword
    if start_date == "today":
        now = datetime.utcnow()
        start_date = now.strftime("%Y-%m-%d")
    if end_date == "today":
        now = datetime.utcnow()
        end_date = now.strftime("%Y-%m-%d")

    api_key = os.getenv("CH_API_KEY")
    if not api_key:
        logger.error("CH_API_KEY environment variable not set")
        sys.exit(1)

    run_for_date_range(start_date, end_date, api_key)
    logging.shutdown()

if __name__ == "__main__":
    main()
