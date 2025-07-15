#!/usr/bin/env python3
"""
scripts/build_frn_profiles.py

Reads the merged FCA JSON slices in docs/fca-dashboard/data/,
and emits one profile file per FRN named <CleanName>.<FRN>.json
in docs/fca-dashboard/data/frn/.
"""
import os
import json
import re

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE = 'docs/fca-dashboard/data'
OUT  = os.path.join(BASE, 'frn')
os.makedirs(OUT, exist_ok=True)

# ─── Load source data ─────────────────────────────────────────────────────────
firms   = json.load(open(os.path.join(BASE, 'fca_firms.json'),            'r', encoding='utf-8'))
names   = json.load(open(os.path.join(BASE, 'fca_names.json'),            'r', encoding='utf-8'))
ars     = json.load(open(os.path.join(BASE, 'fca_ars.json'),              'r', encoding='utf-8'))
cf      = json.load(open(os.path.join(BASE, 'fca_cf.json'),               'r', encoding='utf-8'))
ind_by  = json.load(open(os.path.join(BASE, 'fca_individuals_by_firm.json'),'r', encoding='utf-8'))
persons = json.load(open(os.path.join(BASE, 'fca_persons.json'),          'r', encoding='utf-8'))

# ─── Helper to sanitize names for filenames ──────────────────────────────────
def clean_name(s: str) -> str:
    # collapse whitespace → underscore, strip non-alphanum/underscore
    s = re.sub(r'\s+', '_', s.strip())
    s = re.sub(r'[^A-Za-z0-9_]', '', s)
    return s or 'Unnamed'

# ─── Build profiles ───────────────────────────────────────────────────────────
for frn_str, basic in firms.items():
    frn = str(frn_str)
    profile = {
        "frn": int(frn),
        "basic": basic,
    }

    # trading_names
    profile["trading_names"] = [
        entry["name"]
        for entry in names
        if str(entry.get("frn")) == frn
    ]

    # appointed_reps
    profile["appointed_reps"] = [
        {
            "irn":        e.get("irn"),
            "name":       e.get("name"),
            "start_date": e.get("effective_date"),
            "end_date":   e.get("end_date"),
        }
        for e in ars
        if str(e.get("principal_frn")) == frn
    ]

    # individuals summary
    ind_list = ind_by.get(frn, [])
    profile["individuals"] = [
        {
            "irn":    rec.get("IRN"),
            "name":   rec.get("Name"),
            "status": rec.get("Status"),
        }
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

    # filename: CleanName.FRN.json
    name_part = clean_name(basic.get("name", ""))
    fname = f"{name_part}.{frn}.json"
    out_path = os.path.join(OUT, fname)

    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(profile, f, indent=2, ensure_ascii=False)

    print(f"📝 Wrote profile for FRN {frn} → {fname}")

print("\n✅ All profiles written to docs/fca-dashboard/data/frn/")
