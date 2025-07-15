#!/usr/bin/env python3
"""
scripts/build_frn_profiles.py

Reads the merged FCA JSON slices in docs/fca-dashboard/data/,
normalizes them into dicts keyed by FRN or IRN, and then
emits one profile file per FRN named <CleanName>.<FRN>.json
into docs/fca-dashboard/data/frn/.
"""
import os, json, re, sys

BASE_DIR = os.path.join('docs', 'fca-dashboard', 'data')
OUT_DIR  = os.path.join(BASE_DIR, 'frn')
os.makedirs(OUT_DIR, exist_ok=True)

def load_json(name):
    path = os.path.join(BASE_DIR, f'{name}.json')
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

# 1) Load raw slices
raw_firms   = load_json('fca_firms')
raw_names   = load_json('fca_names')
raw_ars     = load_json('fca_ars')
ind_by      = load_json('fca_individuals_by_firm')
persons     = load_json('fca_persons')
cf          = load_json('fca_cf')

# 2) Normalize firms → dict[frn_str] = { … }
if isinstance(raw_firms, dict):
    firms = { str(k): v for k,v in raw_firms.items() }
else:
    firms = {}
    for entry in raw_firms:
        frn = entry.get('frn') or entry.get('FRN')
        if frn is None: 
            print(f"⚠️  Skipping firm entry without FRN: {entry}", file=sys.stderr)
            continue
        firms[str(frn)] = entry

# 3) Normalize trading‐names → dict[frn_str] = [name,…]
names_map = {}
if isinstance(raw_names, dict):
    # already a mapping frn → [names]
    for k, v in raw_names.items():
        names_map[str(k)] = v if isinstance(v, list) else [v]
else:
    # list of objects
    for e in raw_names:
        frn = e.get('frn') or e.get('FRN')
        name = e.get('name') or e.get('TradingName') or e.get('Name')
        if frn and name:
            names_map.setdefault(str(frn), []).append(name)

# 4) Normalize appointed‐reps → dict[frn_str] = [ {...}, … ]
ars_map = {}
for e in raw_ars:
    frn = e.get('principal_frn') or e.get('frn') or e.get('PRINCIPAL_FRN')
    if not frn: continue
    rec = {
        "irn":        e.get('irn'),
        "name":       e.get('name'),
        "start_date": e.get('effective_date') or e.get('start'),
        "end_date":   e.get('end_date') or e.get('end'),
    }
    ars_map.setdefault(str(frn), []).append(rec)

# Helper to clean names for filenames
def clean_name(s: str) -> str:
    s = re.sub(r'\s+', '_', s.strip())
    return re.sub(r'[^A-Za-z0-9_]', '', s) or 'Unnamed'

# 5) Build one profile per FRN
for frn_str, basic in firms.items():
    profile = {
        "frn": int(frn_str),
        "basic": basic,
        "trading_names":   names_map.get(frn_str, []),
        "appointed_reps":  ars_map.get(frn_str, []),
        "individuals": [
            {"irn": rec["IRN"], "name": rec["Name"], "status": rec["Status"]}
            for rec in ind_by.get(frn_str, [])
            if isinstance(rec, dict)
        ],
        "person_metadata": {
            rec["IRN"]: persons[rec["IRN"]]
            for rec in ind_by.get(frn_str, [])
            if isinstance(rec, dict) and rec.get("IRN") in persons
        },
        "controlled_functions": {
            rec["IRN"]: cf.get(rec["IRN"], {})
            for rec in ind_by.get(frn_str, [])
            if isinstance(rec, dict) and rec.get("IRN")
        }
    }

    fname = f"{clean_name(basic.get('name') or basic.get('Name'))}.{frn_str}.json"
    outp  = os.path.join(OUT_DIR, fname)
    with open(outp, 'w', encoding='utf-8') as f:
        json.dump(profile, f, indent=2, ensure_ascii=False)

    print(f"📝 Wrote profile: {fname}")

print(f"\n✅ All {len(firms)} profiles written to {OUT_DIR}")
