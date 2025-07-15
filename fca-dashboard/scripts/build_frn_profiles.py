#!/usr/bin/env python3
"""
scripts/build_frn_profiles.py

Reads the merged FCA JSON slices in docs/fca-dashboard/data/,
and emits one profile file per FRN named <CleanName>.<FRN>.json
into docs/fca-dashboard/data/frn/.
"""
import os
import json
import re
import sys

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.join('docs', 'fca-dashboard', 'data')
OUT_DIR  = os.path.join(BASE_DIR, 'frn')
os.makedirs(OUT_DIR, exist_ok=True)

# ─── Load & normalize fca_firms.json ─────────────────────────────────────────
firms_path = os.path.join(BASE_DIR, 'fca_firms.json')
with open(firms_path, 'r', encoding='utf-8') as f:
    raw_firms = json.load(f)

# Build a dict { frn_str: entry } whether raw_firms is list or dict
if isinstance(raw_firms, dict):
    firms = raw_firms
elif isinstance(raw_firms, list):
    firms = {}
    for entry in raw_firms:
        # look for FRN field (try lowercase 'frn' or uppercase 'FRN')
        frn_val = entry.get('frn') or entry.get('FRN')
        if frn_val is None:
            print(f"⚠️  Skipping entry without FRN: {entry}", file=sys.stderr)
            continue
        firms[str(frn_val)] = entry
else:
    raise RuntimeError(f"Unexpected type for fca_firms.json: {type(raw_firms)}")

# ─── Load the rest of the source files ────────────────────────────────────────
def load(name):
    path = os.path.join(BASE_DIR, f'{name}.json')
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

names   = load('fca_names')
ars     = load('fca_ars')
ind_by  = load('fca_individuals_by_firm')
persons = load('fca_persons')
cf      = load('fca_cf')

# ─── Helper to sanitize names for filenames ──────────────────────────────────
def clean_name(s: str) -> str:
    s = re.sub(r'\s+', '_', s.strip())
    s = re.sub(r'[^A-Za-z0-9_]', '', s)
    return s or 'Unnamed'

# ─── Build and write profiles ─────────────────────────────────────────────────
for frn_str, basic in firms.items():
    profile = {"frn": int(frn_str), "basic": basic}

    # trading_names
    profile["trading_names"] = [
        rec["name"]
        for rec in names
        if str(rec.get("frn")) == frn_str
    ]

    # appointed_reps
    profile["appointed_reps"] = [
        {
            "irn":        rec.get("irn"),
            "name":       rec.get("name"),
            "start_date": rec.get("effective_date"),
            "end_date":   rec.get("end_date"),
        }
        for rec in ars
        if str(rec.get("principal_frn")) == frn_str
    ]

    # individuals summary
    ind_list = ind_by.get(frn_str, [])
    profile["individuals"] = [
        {"irn": rec.get("IRN"), "name": rec.get("Name"), "status": rec.get("Status")}
        for rec in ind_list
    ]

    # person_metadata lookup
    profile["person_metadata"] = {
        irn: persons[irn]
        for rec in ind_list
        if (irn := rec.get("IRN")) in persons
    }

    # controlled_functions per IRN
    profile["controlled_functions"] = {
        irn: cf.get(irn, {})
        for rec in ind_list
        if (irn := rec.get("IRN"))
    }

    # write out
    name_part = clean_name(basic.get("name", ""))
    filename = f"{name_part}.{frn_str}.json"
    out_path  = os.path.join(OUT_DIR, filename)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(profile, f, indent=2, ensure_ascii=False)

    print(f"📝 Wrote profile: {filename}")

print(f"\n✅ All profiles written under {OUT_DIR}")
