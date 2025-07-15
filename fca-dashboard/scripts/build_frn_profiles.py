#!/usr/bin/env python3
"""
scripts/build_frn_profiles.py

For each FRN‐stub in docs/fca-dashboard/data/frn/*.json, merge in:
  • fca_main.json
  • fca_names.json
  • fca_ars.json
  • fca_individuals_by_firm.json
  • fca_persons.json
  • all CF chunk artifacts under chunks/cf-part-*/…/*.json

Writes each profile JSON in place.
"""
import os
import json
import glob

# ─── PATHS ───────────────────────────────────────────────────────────────────
RAW_DIR     = os.path.abspath('fca-dashboard/data')
PROFILE_DIR = os.path.abspath('docs/fca-dashboard/data/frn')
CF_CHUNKS   = os.path.abspath('chunks')  # where your CF artifacts land

def load_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

# ─── LOAD MASTER SLICES ─────────────────────────────────────────────────────
main_slice    = load_json(os.path.join(RAW_DIR, 'fca_main.json'))                   
names_slice   = load_json(os.path.join(RAW_DIR, 'fca_names.json'))                  
ars_slice     = load_json(os.path.join(RAW_DIR, 'fca_ars.json'))                    
indiv_slice   = load_json(os.path.join(RAW_DIR, 'fca_individuals_by_firm.json'))    
persons_slice = load_json(os.path.join(RAW_DIR, 'fca_persons.json'))                

# ─── MERGE CF CHUNKS IN MEMORY ─────────────────────────────────────────────────
cf_by_irn = {}
for chunk in glob.glob(os.path.join(CF_CHUNKS, 'cf-part-*', '*.json')):
    try:
        data = load_json(chunk)
    except Exception:
        continue
    if not isinstance(data, dict):
        continue
    for irn, entries in data.items():
        cf_by_irn.setdefault(irn, []).extend(entries or [])
print(f"🔄 Loaded CF for {len(cf_by_irn)} IRNs from {CF_CHUNKS}")

# ─── BUILD/UPDATE PROFILES ───────────────────────────────────────────────────
for profile_path in glob.glob(os.path.join(PROFILE_DIR, '*.json')):
    profile = load_json(profile_path)
    frn = str(profile.get('frn'))

    # 1) Firm metadata
    profile['metadata'] = main_slice.get(frn, {})

    # 2) Trading names
    profile['trading_names'] = names_slice.get(frn, [])

    # 3) Appointed reps
    profile['appointed_reps'] = ars_slice.get(frn, [])

    # 4) List of IRNs
    indivs = indiv_slice.get(frn, [])
    profile['individuals'] = indivs

    # 5) Per-IRN person records
    person_records = {}
    for rec in indivs:
        irn = str(rec.get('IRN'))
        if irn in persons_slice:
            person_records[irn] = persons_slice[irn]
    profile['person_records'] = person_records

    # 6) Per-IRN CF entries
    controlled_functions = {
        irn: cf_by_irn.get(irn, [])
        for irn in person_records
    }
    profile['controlled_functions'] = controlled_functions

    # Write back
    with open(profile_path, 'w', encoding='utf-8') as f:
        json.dump(profile, f, indent=2, ensure_ascii=False)
    print(f"✅ Updated {os.path.basename(profile_path)}")

print("\n🎉 All FRN profiles merged.")
