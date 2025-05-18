import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import json
import time
import argparse

# --- CONFIGURATION ---
API_KEY = os.getenv('CH_API_KEY')
DAILY_UPDATE_INTERVAL_MINUTES = 10

MASTER_FILE = 'master_companies.xlsx'
PAGINATION_TRACKER = 'pagination_tracker.json'
LOG_FILE = 'update_log.csv'

SIC_CODES = [
    '66300', '64999', '64301', '64304', '64305', '64306', '64205', '66190', '70100'
]

KEYWORDS = ['capital', 'fund', 'ventures', 'partners', 'gp', 'lp', 'llp', 'investments', 'equity', 'advisors']

COLUMNS = ['Company Name', 'Company Number', 'Incorporation Date', 'Status', 'Source', 'Time Discovered']

def load_json_file(path):
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    return {}

def save_json_file(data, path):
    with open(path, 'w') as f:
        json.dump(data, f)

def fetch_companies_for_date(api_key, query_date, sic_codes, keywords, last_index=0):
    base_url = 'https://api.company-information.service.gov.uk/advanced-search/companies'
    headers = {'Authorization': api_key}
    all_results = []
    start_index = last_index

    while True:
        params = {
            'incorporated_from': query_date,
            'incorporated_to': query_date,
            'start_index': start_index,
            'size': 50
        }
        response = requests.get(base_url, headers=headers, params=params)
        if response.status_code != 200:
            break

        data = response.json()
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
    else:
        return pd.DataFrame(columns=COLUMNS + ['Date Downloaded'])

def update_master(master_df, new_df):
    today_str = datetime.today().strftime('%Y-%m-%d')
    new_entries = new_df[~new_df['Company Number'].isin(master_df['Company Number'])].copy()
    new_entries['Date Downloaded'] = today_str
    updated_master = pd.concat([master_df, new_entries], ignore_index=True)
    updated_master.drop_duplicates(subset='Company Number', inplace=True)
    return updated_master, new_entries

def export_to_excel(df, filename):
    df.to_excel(filename, index=False)

def log_update(date, added_count):
    if added_count == 0:
        return
    log_line = f"{date},{added_count},{datetime.now().strftime('%H:%M:%S')}\n"
    header = "Date,Companies Added,Run Time\n"
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'w') as f:
            f.write(header)
    with open(LOG_FILE, 'a') as f:
        f.write(log_line)

def run_for_date(query_date):
    pagination_tracker = load_json_file(PAGINATION_TRACKER)
    last_index = pagination_tracker.get(query_date, 0)
    new_discoveries, final_index = fetch_companies_for_date(API_KEY, query_date, SIC_CODES, KEYWORDS, last_index)

    pagination_tracker[query_date] = final_index
    save_json_file(pagination_tracker, PAGINATION_TRACKER)

    if os.path.exists(PAGINATION_TRACKER) and os.path.getmtime(PAGINATION_TRACKER) < time.time() - 30 * 86400:
        os.remove(PAGINATION_TRACKER)

    master_df = load_existing_master(MASTER_FILE)
    updated_master_df, newly_added = update_master(master_df, new_discoveries)

    export_to_excel(updated_master_df, MASTER_FILE)
    log_update(query_date, len(newly_added))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Companies House tracker.")
    parser.add_argument("--date", type=str, help="Optional date to fetch (YYYY-MM-DD). Defaults to today.")
    args = parser.parse_args()

    query_date = args.date or datetime.today().strftime('%Y-%m-%d')
    run_for_date(query_date)
