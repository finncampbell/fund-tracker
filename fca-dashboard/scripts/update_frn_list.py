#!/usr/bin/env python3
import os
import re
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup

# 1) URL of FCA Resources page
RESOURCES_URL = 'https://register.fca.org.uk/s/resources'

# 2) Data directory to store JSON and raw CSVs
SCRIPT_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(SCRIPT_DIR, '../data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')
OUT_JSON = os.path.join(DATA_DIR, 'all_frns_with_names.json')

# 3) Registers to include (match by link text)
REGISTER_KEYWORDS = [
    'Investment Firms Register',
    'Register of Small Registered UK AIFMs',
]

def fetch_csv_links():
    """Scrape the Resources page and return list of {name, url}."""
    resp = requests.get(RESOURCES_URL, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    links = []
    for a in soup.select('a[href$=".csv"]'):
        text = a.get_text(strip=True)
        if any(kw in text for kw in REGISTER_KEYWORDS):
            href = a['href']
            url = href if href.startswith('http') else 'https://register.fca.org.uk' + href
            links.append({'name': text, 'url': url})
    return links

def download_and_extract(link):
    """Download one CSV, save raw file, and return list of (frn,name)."""
    print(f"Downloading: {link['name']} → {link['url']}")
    r = requests.get(link['url'], timeout=10)
    r.raise_for_status()

    # --- ensure raw directory exists and save CSV ---
    os.makedirs(RAW_DIR, exist_ok=True)
    fname = re.sub(r'[^A-Za-z0-9]+', '_', link['name']).strip('_') + '.csv'
    raw_path = os.path.join(RAW_DIR, fname)
    with open(raw_path, 'w', encoding='utf-8') as f:
        f.write(r.text)
    print(f"Saved raw CSV to {raw_path}")

    # parse with pandas
    df = pd.read_csv(pd.compat.StringIO(r.text), dtype=str)
    frn_col = next(c for c in df.columns if re.search(r'\bfrn\b', c, re.I))
    name_col = next(c for c in df.columns if re.search(r'\bname\b', c, re.I))
    df = df[[frn_col, name_col]].dropna(subset=[frn_col])
    df[frn_col] = df[frn_col].str.strip()
    df[name_col] = df[name_col].str.strip()
    return list(df.itertuples(index=False, name=None))

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # 1) Scrape CSV links
    links = fetch_csv_links()
    print(f"Found {len(links)} matching CSV links.")

    # 2) Download + extract FRN/name
    mapping = {}
    for link in links:
        for frn, name in download_and_extract(link):
            mapping[frn] = name

    # 3) Write out JSON array of objects
    out = [{"frn": frn, "name": name} for frn, name in sorted(mapping.items())]
    with open(OUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2)
    print(f"Wrote {len(out)} entries to {OUT_JSON}")

if __name__ == '__main__':
    main()
