#!/usr/bin/env python3
import os
import sys
import argparse
from datetime import datetime, timedelta

import pandas as pd

from logger import log
from config import COLUMN_SCHEMA
from fetch import fetch_companies_for_date
from enrich import classify, enrich_sic, has_target_sic

LOG = log

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

def ingest(start: str, end: str) -> pd.DataFrame:
    records = []
    for ds in date_range(start, end):
        LOG.info(f"→ Fetching {ds}")
        items = fetch_companies_for_date(ds)
        LOG.info(f"   • {len(items)} records")
        now = datetime.utcnow()
        for c in items:
            records.append({
                "CompanyNumber":     c.get("company_number", ""),
                "CompanyName":       c.get("title", c.get("company_name", "")),
                "IncorporationDate": c.get("date_of_creation", ""),
                "Status":            c.get("company_status", ""),
                "Source":            c.get("source", ""),
                "DateDownloaded":    now.date().isoformat(),
                "TimeDiscovered":    now.isoformat(),
                "SIC Codes":         ",".join(c.get("sic_codes", [])),
                "Category":          "",
                "SIC Description":   "",
                "Typical Use Case":  "",
            })
    return pd.DataFrame(records, columns=COLUMN_SCHEMA)

def enrich_and_filter(df: pd.DataFrame) -> pd.DataFrame:
    df["Category"] = df["CompanyName"].apply(classify)
    sic_enriched = df["SIC Codes"].apply(lambda s: enrich_sic(s.split(","))).tolist()
    df[["SIC Description", "Typical Use Case"]] = sic_enriched
    mask_cat = df["Category"] != "Other"
    mask_sic = df["SIC Codes"].apply(has_target_sic)
    return df[mask_cat | mask_sic]

def write_outputs(df_master: pd.DataFrame, df_rel: pd.DataFrame):
    os.makedirs("docs/assets/data", exist_ok=True)

    # Master
    master_csv  = "docs/assets/data/master_companies.csv"
    master_xlsx = "docs/assets/data/master_companies.xlsx"
    df_master.to_csv(master_csv, index=False)
    with pd.ExcelWriter(master_xlsx, engine="xlsxwriter") as w:
        df_master.to_excel(w, index=False, sheet_name="master")
    LOG.info(f"Wrote master ({len(df_master)}) records")

    # Relevant
    rel_csv  = "docs/assets/data/relevant_companies.csv"
    rel_xlsx = "docs/assets/data/relevant_companies.xlsx"
    df_rel.to_csv(rel_csv, index=False)
    with pd.ExcelWriter(rel_xlsx, engine="xlsxwriter") as w:
        df_rel.to_excel(w, index=False, sheet_name="relevant")
    LOG.info(f"Wrote relevant ({len(df_rel)}) records")

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

    df_raw = ingest(start, end)
    if df_raw.empty:
        LOG.warning("No records fetched for %s → %s; writing empty outputs.", start, end)
        # build an empty relevant DataFrame with same schema
        df_rel = pd.DataFrame(columns=COLUMN_SCHEMA)
        write_outputs(df_raw, df_rel)
        return

    df_rel = enrich_and_filter(df_raw)
    write_outputs(df_raw, df_rel)

if __name__ == "__main__":
    main()
