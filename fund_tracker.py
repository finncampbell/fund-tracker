import os
import json
import time
import logging
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import argparse

# --- CONFIGURATION ---
API_KEY = os.getenv('CH_API_KEY')
MASTER_FILE = 'master_companies.xlsx'
PAGINATION_TRACKER = 'pagination_tracker.json'
API_LOG_FILE = 'api_logs.json'
DATA_CSV_PUBLIC = 'assets/data/master_companies.csv'   # <-- public for DataTables/Jekyll
DATA_CSV_JEKYLL = '_data/master_companies.csv'         # <-- optional, for Jekyll/Liquid use
SIC_CODES = [
    '66300', '64999', '64301', '64304', '64305', '64306', '64205', '66190', '70100'
]
KEYWORDS = [
    'capital', 'fund', 'ventures', 'partners', 'gp', 'lp', 'llp',
    'investments', 'equity', 'advisors'
]
COLUMNS = [
    'Company Name', 'Company Number', 'Incorporation Date',
    'Status', 'Source', 'Time Discovered'
]

# Setup structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(API_LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# HTTP retry strategy
retry_strategy = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    backoff_factor=1
)
session = requests.Session()
session.mount('https://', HTTPAdapter(max_retries=retry_strategy))
session.auth = (API_KEY, '')

def load_json_file(path):
    """Load JSON or return empty dict if missing or malformed."""
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            logger.warning(f"Corrupt JSON in {path}, resetting.")
            return {}
    return {}

def save_json_file(data, path):
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)

def fetch_companies_for_date(query_date, last_index=0):
    url = 'https://api.company-information.service.gov.uk/advanced-search/companies'
    all_results = []
    start_index = last_index

    while True:
        params = {
            'incorporated_from': query_date,
            'incorporated_to': query_date,
            'start_index': start_index,
            'size': 50
        }
        try:
            resp = session.get(url, params=params)
            resp.raise_for_status()
        except requests.HTTPError as e:
            logger.error(f"API error on {query_date} at index {start_index}: {e}")
            break

        data = resp.json()
        items = data.get('items', [])
        if not items:
            break

        for item in items:
            name = item.get('company_name', '').lower()
            matched_by = []
            if any(code in SIC_CODES for code in item.get('sic_codes', [])):
                matched_by.append('SIC')
            if any(kw in name for kw in KEYWORDS):
                matched_by.append('Keyword')
            if matched_by:
                all_results.append({
                    'Company Name': item.get('company_name'),
                    'Company Number': item.get('company_number'),
                    'Incorporation Date': item.get('date_of_creation'),
                    'Status': item.get('company_status'),
                    'Source': '+'.join(sorted(set(matched_by))),
                    'Time Discovered': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

        start_index += 50
        time.sleep(0.2)
        if len(items) < 50:
            break

    return pd.DataFrame(all_results, columns=COLUMNS), start_index

def load_existing_master(path):
    if os.path.exists(path):
        return pd.read_excel(path)
    return pd.DataFrame(columns=COLUMNS + ['Date Downloaded'])

def update_master(master_df, new_df):
    today_str = datetime.today().strftime('%Y-%m-%d')
    new_entries = new_df[~new_df['Company Number'].isin(master_df['Company Number'])].copy()
    new_entries['Date Downloaded'] = today_str
    updated = pd.concat([master_df, new_entries], ignore_index=True)
    updated.drop_duplicates(subset='Company Number', inplace=True)
    return updated, new_entries

def export_to_excel(df, filename):
    df.to_excel(filename, index=False)

def log_update(date, added_count):
    logs = load_json_file(API_LOG_FILE)
    entry = {
        "Date": date,
        "Added": added_count,
        "Time": datetime.now().strftime('%H:%M:%S')
    }
    logs.setdefault(date, []).append(entry)
    save_json_file(logs, API_LOG_FILE)

def run_for_date_range(start_date, end_date):
    pagination = load_json_file(PAGINATION_TRACKER)
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    while current <= end:
        dstr = current.strftime('%Y-%m-%d')
        last = pagination.get(dstr, 0)
        df_new, new_index = fetch_companies_for_date(dstr, last)
        pagination[dstr] = new_index
        save_json_file(pagination, PAGINATION_TRACKER)

        master = load_existing_master(MASTER_FILE)
        updated, added = update_master(master, df_new)
        export_to_excel(updated, MASTER_FILE)
        log_update(dstr, len(added))

        # Export for dashboard (public)
        if not os.path.exists(os.path.dirname(DATA_CSV_PUBLIC)):
            os.makedirs(os.path.dirname(DATA_CSV_PUBLIC), exist_ok=True)
        updated.to_csv(DATA_CSV_PUBLIC, index=False)

        # Optionally also for Jekyll/Liquid use
        if not os.path.exists(os.path.dirname(DATA_CSV_JEKYLL)):
            os.makedirs(os.path.dirname(DATA_CSV_JEKYLL), exist_ok=True)
        updated.to_csv(DATA_CSV_JEKYLL, index=False)

        # Cleanup old tracker
        if os.path.getmtime(PAGINATION_TRACKER) < time.time() - 30 * 86400:
            os.remove(PAGINATION_TRACKER)

        current += timedelta(days=1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Companies House tracker.")
    parser.add_argument(
        "--start_date",
        type=str,
        help="YYYY-MM-DD or literal 'today'",
        default="today"
    )
    parser.add_argument(
        "--end_date",
        type=str,
        help="YYYY-MM-DD or literal 'today'",
        default="today"
    )
    args = parser.parse_args()

    # Interpret “today” placeholders
    today_str = datetime.today().strftime('%Y-%m-%d')
    sd = today_str if args.start_date.lower() == 'today' else args.start_date
    ed = today_str if args.end_date.lower() == 'today' else args.end_date

    logger.info(f"Starting run: {sd} to {ed}")
    run_for_date_range(sd, ed)
