#!/usr/bin/env python3
import os
import sys
import argparse
from datetime import datetime, timedelta

import pandas as pd

from logger import log as LOG
from config import COLUMN_SCHEMA
from fetch import fetch_companies_on

def normalize_date(s: str) -> str:
    today = datetime.utcnow().date()
    if s.lower() == "today":
        return today.isoformat()
    if s.lower() == "yesterday":
        return (today - timedelta(days=1)).isoformat()
    return datetime.fromisoformat(s).date().isoformat()

def date_range(start: str, end: str):
    d0 = datetime.fromisoformat(start).date()
    d1 = datetime.fromisoformat(end).date()
    for i in range((d1 - d0).days + 1):
        yield (d0 + timedelta(days=i)).isoformat()

def run_for_date_range(start_date: str, end_date: str):
    sd = datetime.fromisoformat(start_date)
    ed = datetime.fromisoformat(end_date)
    if sd > ed:
        LOG.error("start_date cannot be after end_date"); sys.exit(1)

    # 1) Fetch *all* new records via paginated Advanced-Search
    new_records = []
    cur = sd
    while cur <= ed:
        ds = cur.date().isoformat()
        LOG.info(f'Fetching companies for {ds}')
        new_records.extend(fetch_companies_on(ds))
        cur += timedelta(days=1)

    # 2) Load or init existing master
    master_csv = "docs/assets/data/master_companies.csv"
    if os.path.exists(master_csv):
        try:
            df_master = pd.read_csv(master_csv)
        except pd.errors.EmptyDataError:
            df_master = pd.DataFrame(columns=COLUMN_SCHEMA)
    else:
        df_master = pd.DataFrame(columns=COLUMN_SCHEMA)

    # 3) Append & dedupe
    if new_records:
        df_new = pd.DataFrame(new_records, columns=COLUMN_SCHEMA)
        df_all = pd.concat([df_master, df_new], ignore_index=True)
        df_all.drop_duplicates(subset=["CompanyNumber"], keep="first", inplace=True)
    else:
        df_all = df_master

    df_all.sort_values("IncorporationDate", ascending=False, inplace=True)

    # 4) Write full master
    os.makedirs(os.path.dirname(master_csv), exist_ok=True)
    df_all.to_csv(master_csv, index=False)
    master_xlsx = master_csv.replace(".csv", ".xlsx")
    with pd.ExcelWriter(master_xlsx, engine="xlsxwriter") as w:
        df_all.to_excel(w, index=False, sheet_name="master")
    LOG.info(f"Wrote master ({len(df_all)}) rows")

    # 5) Post-hoc filter for relevant
    # Ensure Category has no stray whitespace
    df_all["Category"] = df_all["Category"].astype(str).str.strip()
    mask_cat = df_all["Category"] != "Other"

    # Replace NaN or missing SIC Description with "" before casting to bool
    mask_sic = df_all["SIC Description"].fillna("").astype(str).str.strip().astype(bool)

    df_rel = df_all[mask_cat | mask_sic]

    # 6) Write relevant slice
    rel_csv = "docs/assets/data/relevant_companies.csv"
    df_rel.to_csv(rel_csv, index=False)
    rel_xlsx = rel_csv.replace(".csv", ".xlsx")
    with pd.ExcelWriter(rel_xlsx, engine="xlsxwriter") as w:
        df_rel.to_excel(w, index=False, sheet_name="relevant")
    LOG.info(f"Wrote relevant ({len(df_rel)}) rows")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--start_date", default="today",
                   help="YYYY-MM-DD, 'today', or 'yesterday'")
    p.add_argument("--end_date",   default="today",
                   help="YYYY-MM-DD, 'today', or 'yesterday'")
    return p.parse_args()

def main():
    args  = parse_args()
    start = normalize_date(args.start_date)
    end   = normalize_date(args.end_date)
    LOG.info(f"Running ingest from {start} to {end}")
    run_for_date_range(start, end)

if __name__ == "__main__":
    main()
