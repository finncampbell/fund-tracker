import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import json
import time
import subprocess

# Debug: confirm environment variables are accessible
API_KEY = os.getenv('CH_API_KEY')
print("CH_API_KEY present:", bool(API_KEY))
print("GH_FUNDTOKEN present:", bool(os.getenv('GH_FUNDTOKEN')))

# --- CONFIGURATION ---
INITIAL_SWEEP_DAYS = 7
DAILY_UPDATE_INTERVAL_MINUTES = 10

MASTER_FILE = 'master_companies.xlsx'
DAILY_FILE_TEMPLATE = 'new_companies_{date}.xlsx'
PAGINATION_TRACKER = 'pagination_tracker.json'
INITIAL_SWEEP_LOG = 'initial_sweep_log.json'
LOG_FILE = 'update_log.csv'

SIC_CODES = [
    '66300', '64999', '64301', '64304', '64305', '64306', '64205', '66190', '70100'
]

KEYWORDS = ['capital', 'fund', 'ventures', 'partners', 'gp', 'lp', 'llp', 'investments', 'equity', 'advisors']

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

    return pd.DataFrame(all_results), start_index

def load_existing_master(path):
    if os.path.exists(path):
        return pd.read_excel(path)
    else:
        return pd.DataFrame(columns=['Company Name', 'Company Number', 'Incorporation Date', 'Status', 'Source', 'Date Downloaded', 'Time Discovered'])

def update_master(master_df, new_df):
    today_str = datetime.today().strftime('%Y-%m-%d')
    new_entries = new_df[~new_df['Company Number'].isin(master_df['Company Number'])].copy()
    new_entries['Date Downloaded'] = today_str
    updated_master = pd.concat([master_df, new_entries], ignore_index=True)
    return updated_master, new_entries

def export_to_excel(df, filename):
    df.to_excel(filename, index=False)

def log_update(date, added_count):
    log_line = f"{date},{added_count},{datetime.now().strftime('%H:%M:%S')}\n"
    header = "Date,Companies Added,Run Time\n"
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'w') as f:
            f.write(header)
    with open(LOG_FILE, 'a') as f:
        f.write(log_line)

def push_to_github():
    print("GH_FUNDTOKEN length:", len(os.getenv('GH_FUNDTOKEN') or ''))
    subprocess.run(["git", "config", "--global", "user.email", "bot@example.com"])
    subprocess.run(["git", "config", "--global", "user.name", "GH Actions Bot"])

    tracked_files = [MASTER_FILE]
    if os.path.exists(LOG_FILE):
        tracked_files.append(LOG_FILE)

    subprocess.run(["git", "add"] + tracked_files)
    try:
        subprocess.run(["git", "commit", "-m", f"Update on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"], check=True)
        subprocess.run(["git", "push", f"https://x-access-token:{os.getenv('GH_FUNDTOKEN')}@github.com/finncampbell/fund-tracker.git"])
    except subprocess.CalledProcessError:
        pass  # No changes to commit

if not API_KEY or not os.getenv('GH_FUNDTOKEN'):
    raise EnvironmentError("Missing CH_API_KEY or GH_FUNDTOKEN environment variables.")

if __name__ == "__main__":
    while True:
        today = datetime.today().strftime('%Y-%m-%d')
        pagination_tracker = load_json_file(PAGINATION_TRACKER)
        initial_sweep_log = load_json_file(INITIAL_SWEEP_LOG)
        new_discoveries = pd.DataFrame()

        for days_ago in range(INITIAL_SWEEP_DAYS, -1, -1):
            query_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
            if days_ago > 0 and initial_sweep_log.get(query_date):
                continue

            last_index = pagination_tracker.get(query_date, 0)
            df_fetched, final_index = fetch_companies_for_date(API_KEY, query_date, SIC_CODES, KEYWORDS, last_index)
            if not df_fetched.empty:
                new_discoveries = pd.concat([new_discoveries, df_fetched], ignore_index=True)

            if days_ago > 0:
                initial_sweep_log[query_date] = True

            pagination_tracker[query_date] = final_index

        save_json_file(pagination_tracker, PAGINATION_TRACKER)
        save_json_file(initial_sweep_log, INITIAL_SWEEP_LOG)

        # Cleanup old JSON files if needed (example: remove logs older than 30 days)
        for file in [PAGINATION_TRACKER, INITIAL_SWEEP_LOG]:
            if os.path.exists(file) and os.path.getmtime(file) < time.time() - 30 * 86400:
                os.remove(file)

        master_df = load_existing_master(MASTER_FILE)
        updated_master_df, newly_added = update_master(master_df, new_discoveries)

        export_to_excel(updated_master_df, MASTER_FILE)
        # Skipping daily export as only master file is needed

        log_update(today, len(newly_added))
        push_to_github()
        time.sleep(DAILY_UPDATE_INTERVAL_MINUTES * 60)
