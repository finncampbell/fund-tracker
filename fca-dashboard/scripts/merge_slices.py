#!/usr/bin/env python3
"""
scripts/merge_slices.py

Merge all FCA slice JSONs into a single fca_firms.json (for the dashboard)
and regenerate fca_dashboard_full.csv for export.
"""

import os
import json
import csv

# ─── Paths ─────────────────────────────────────────────────────────────────────
DATA_DIR     = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
MAIN_JSON    = os.path.join(DATA_DIR, "fca_main.json")
NAMES_JSON   = os.path.join(DATA_DIR, "fca_names.json")
ARS_JSON     = os.path.join(DATA_DIR, "fca_ars.json")
CF_JSON      = os.path.join(DATA_DIR, "fca_cf.json")
INDIV_JSON   = os.path.join(DATA_DIR, "fca_individuals_by_firm.json")
PERSONS_JSON = os.path.join(DATA_DIR, "fca_persons.json")

OUTPUT_JSON  = os.path.join(DATA_DIR, "fca_firms.json")
OUTPUT_CSV   = os.path.join(DATA_DIR, "fca_dashboard_full.csv")


def load_json(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    # Load every slice
    main_store    = load_json(MAIN_JSON)    # dict: frn -> {core fields}
    names_store   = load_json(NAMES_JSON)   # dict: frn -> [names]
    ars_store     = load_json(ARS_JSON)     # dict: frn -> [ar entries]
    cf_store      = load_json(CF_JSON)      # dict: frn -> [cf entries]
    indiv_store   = load_json(INDIV_JSON)   # dict: frn -> [individual entries]
    persons_store = load_json(PERSONS_JSON) # dict: irn -> {person fields}

    # Build merged JSON array
    merged = []
    for frn, core in main_store.items():
        entry = dict(core)  # copy core fields

        # Attach slices
        entry["trading_names"]    = [n for n in names_store.get(frn, []) if n]
        entry["appointed_reps"]   = ars_store.get(frn, [])
        entry["controlled_functions"] = cf_store.get(frn, [])
        entry["firm_individuals"] = indiv_store.get(frn, [])

        # If you want persons embedded:
        # entry["individual_records"] = [
        #     persons_store.get(ind["IRN"], {}) for ind in entry["firm_individuals"]
        # ]

        merged.append(entry)

    # Write merged JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    print(f"✅ Wrote merged JSON: {OUTPUT_JSON} ({len(merged)} firms)")

    # Also regenerate CSV (optional; you can drop this if you only need JSON)
    fieldnames = [
        "frn",
        "organisation_name",
        "status",
        "business_type",
        "companies_house_number",
        "system_timestamp",
        "status_effective_date",
        "trading_names",
        "ars_count",
        "cf_current_count",
        "cf_previous_count",
        "individuals_count",
        # add more CSV columns as you wish...
    ]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as cf:
        writer = csv.DictWriter(cf, fieldnames=fieldnames)
        writer.writeheader()
        for e in merged:
            cf_current  = sum(1 for c in e["controlled_functions"] if c.get("section") == "Current")
            cf_previous = sum(1 for c in e["controlled_functions"] if c.get("section") == "Previous")
            row = {
                "frn":                     e.get("frn", ""),
                "organisation_name":       e.get("organisation_name", ""),
                "status":                  e.get("status", ""),
                "business_type":           e.get("business_type", ""),
                "companies_house_number":  e.get("companies_house_number", ""),
                "system_timestamp":        e.get("system_timestamp", ""),
                "status_effective_date":   e.get("status_effective_date", ""),
                "trading_names":           ";".join(e.get("trading_names", [])),
                "ars_count":               len(e.get("appointed_reps", [])),
                "cf_current_count":        cf_current,
                "cf_previous_count":       cf_previous,
                "individuals_count":       len(e.get("firm_individuals", [])),
            }
            writer.writerow(row)
    print(f"✅ Wrote merged CSV:  {OUTPUT_CSV} ({len(merged)} rows)")


if __name__ == "__main__":
    main()
