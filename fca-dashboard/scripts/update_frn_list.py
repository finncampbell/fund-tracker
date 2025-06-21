#!/usr/bin/env python3
import os
import re
import json
import requests
import pandas as pd
from io import StringIO

# 1) Hard‑coded download URLs for the two registers
LINKS = [
    {
        'name': 'Investment Firms Register',
        'url': 'https://register.fca.org.uk/servlet/servlet.FileDownload?file=0150X000006gbb6'
    },
    {
        'name': 'Register of Small Registered UK AIFMs',
        'url': 'https://register.fca.org.uk/servlet/servlet.FileDownload?file=015Sk000000Tfm9'
    }
]

# 2) Where to write raw CSVs and the merged JSON
SCRIPT_DIR = os.path.dirname(__file__)
DATA_DIR   = os.path.join(SCRIPT_DIR, '../data')
RAW_DIR    = os.path.join(DATA_DIR, 'raw')
OUT_JSON   = os.path.join(DATA_DIR, 'all_frns_with_names.json')

def download_and_extract(link):
    """Download one CSV, save raw file, and return list of (frn, name)."""
    print(f"Downloading {link['name']} → {link['url']}")
    resp = requests.get(link['url'], timeout=15)
    resp.raise_for_status()
    text = resp.text

    # save raw CSV
    os.makedirs(RAW_DIR, exist_ok=True)
    safe_name = re.sub(r'[^A-Za-z0-9]+', '_', link['name']).strip('_') + '.csv'
    raw_path = os.path.join(RAW_DIR, safe_name)
    with open(raw_path, 'w', encoding='utf-8') as f:
        f.write(text)
    print(f"  • saved raw CSV to {raw_path}")

    # parse into DataFrame
    df = pd.read_csv(StringIO(text), dtype=str)
    # detect FRN and name columns
    frn_col  = next(c for c in df.columns if re.search(r'\bfrn\b', c, re.I))
    name_col = next(c for c in df.columns if re.search(r'\bname\b', c, re.I))
    # extract and clean
    df = df[[frn_col, name_col]].dropna(subset=[frn_col])
    df[frn_col]  = df[frn_col].str.strip()
    df[name_col] = df[name_col].str.strip()
    return list(df.itertuples(index=False, name=None))

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    mapping = {}
    for link in LINKS:
        for frn, name in download_and_extract(link):
            mapping[frn] = name

    # write merged JSON
    out = [{"frn": frn, "name": name} for frn, name in sorted(mapping.items())]
    with open(OUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2)
    print(f"Wrote {len(out)} entries to {OUT_JSON}")

if __name__ == '__main__':
    main()
