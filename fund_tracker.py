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
LOG_FILE = 'fund_tracker.log'
DATA_CSV_PUBLIC = 'assets/data/master_companies.csv'
SIC_CODES = [
    '66300', '64999', '64301', '64304', '64305', '64306', '64205', '66190', '70100'
]
KEYWORDS = [
    'capital', 'fund', 'ventures', 'partners', 'gp', 'lp', 'llp',
    'investments', 'equity', 'advisors'
]
COLUMNS = [
    'Company Name', 'Company Number', 'Incorporation Date',
    'Status', 'Source', 'Date Downloaded', 'Time Discovered'
]

# Setup structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
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
        except requests.RequestException as e:
            logger.error(f"API request error on {query_date} at index {start_index}: {e}")
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
                    'Date Downloaded': datetime.today().strftime('%Y-%m-%d'),
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
    return pd.DataFrame(columns=COLUMNS)

def update_master(master_df, new_df):
    new_entries = new_df[~new_df['Company Number'].isin(master_df['Company Number'])].copy()
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
        last_index = pagination.get(dstr, 0)

        df_new, new_index = fetch_companies_for_date(dstr, last_index)
        pagination[dstr] = new_index
        save_json_file(pagination, PAGINATION_TRACKER)

        master_df = load_existing_master(MASTER_FILE)
        updated_df, added_df = update_master(master_df, df_new)
        export_to_excel(updated_df, MASTER_FILE)
        log_update(dstr, len(added_df))

        # Reorder & export only public CSV for JS dashboard
        os.makedirs(os.path.dirname(DATA_CSV_PUBLIC), exist_ok=True)
        export_cols = [
            'Company Name', 'Company Number', 'Incorporation Date',
            'Status', 'Source', 'Date Downloaded', 'Time Discovered'
        ]
        updated_df = updated_df[export_cols]
        updated_df.to_csv(DATA_CSV_PUBLIC, index=False)

        # Cleanup old tracker
        if os.path.getmtime(PAGINATION_TRACKER) < time.time() - 30 * 86400:
            os.remove(PAGINATION_TRACKER)

        current += timedelta(days=1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Companies House tracker.")
    parser.add_argument("--start_date", type=str, default="today",
                        help="YYYY-MM-DD or literal 'today'")
    parser.add_argument("--end_date", type=str, default="today",
                        help="YYYY-MM-DD or literal 'today'")
    args = parser.parse_args()

    today_str = datetime.today().strftime('%Y-%m-%d')
    sd = today_str if args.start_date.lower() == 'today' else args.start_date
    ed = today_str if args.end_date.lower() == 'today' else args.end_date

    logger.info(f"Starting run: {sd} to {ed}")
    run_for_date_range(sd, ed)
