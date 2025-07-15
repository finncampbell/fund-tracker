#!/usr/bin/env python3
"""
scripts/build_frn_profiles.py

For each profile skeleton in docs/fca-dashboard/data/frn/*.json,
merge in data from the various FCA slices:

  • fca-dashboard/data/fca_main.json           → main firm metadata
  • fca-dashboard/data/fca_names.json          → trading names
  • fca-dashboard/data/fca_ars.json            → appointed reps
  • fca-dashboard/data/fca_individuals_by_firm.json
                                                → firm→list of IRNs
  • fca-dashboard/data/fca_persons.json        → per-IRN person_metadata
  • fca-dashboard/data/fca_cf.json             → per-IRN controlled functions

Writes back each profile JSON in place.
"""
import os
import json
import glob

# ─── Paths ───────────────────────────────────────────────────────────────────
RAW_DIR    = os.path.join('fca-dashboard', 'data')
PROF_DIR   = os.path.join('docs', 'fca-dashboard', 'data', 'frn')

# ─── Load all slices once ─────────────────────────────────────────────────────
def load_json(name):
    path = os.path.join(RAW_DIR, name)
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

print("⏳ Loading raw FCA data slices…")
main_slice      = load_json('fca_main.json')                           # {frn: {...}}
names_slice     = load_json('fca_names.json')                          # {frn: [name, …]}
ars_list        = load_json('fca_ars.json')                            # [ { principal_frn, … }, … ]
indiv_by_firm   = load_json('fca_individuals_by_firm.json')            # {frn: [ {IRN,…},… ], …}
persons_metadata= load_json('fca_persons.json')                         # { irn: {...}, … }
cf_list         = load_json('fca_cf.json')                              # [ { IRN, …CF fields… }, … ]

# Build quick lookups
print("🔨 Indexing ARs and CFs…")
ars_by_firm = {}
for rec in ars_list:
    frn = str(rec.get('principal_frn') or rec.get('FRN'))
    ars_by_firm.setdefault(frn, []).append(rec)

cf_by_irn = {}
for rec in cf_list:
    irn = str(rec.get('IRN'))
    cf_by_irn.setdefault(irn, []).append(rec)

# ─── Process each skeleton profile ───────────────────────────────────────────
skeletons = glob.glob(os.path.join(PROF_DIR, '*.json'))
print(f"⚙️  Found {len(skeletons)} profile files to fill…")

for path in skeletons:
    with open(path, 'r', encoding='utf-8') as f:
        profile = json.load(f)

    frn_str = str(profile.get('frn'))
    # 1) main metadata (if present)
    profile['main'] = main_slice.get(frn_str, {})

    # 2) trading names
    profile['trading_names'] = names_slice.get(frn_str, [])

    # 3) appointed reps
    profile['appointed_reps'] = ars_by_firm.get(frn_str, [])

    # 4) individuals list
    indivs = indiv_by_firm.get(frn_str, [])
    profile['individuals'] = indivs

    # 5) person_metadata per IRN
    pm = {}
    for rec in indivs:
        irn = str(rec.get('IRN'))
        if irn in persons_metadata:
            pm[irn] = persons_metadata[irn]
    profile['person_metadata'] = pm

    # 6) controlled_functions per IRN
    cf = {}
    for irn in pm.keys():
        cf[irn] = cf_by_irn.get(irn, [])
    profile['controlled_functions'] = cf

    # write back
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(profile, f, indent=2, ensure_ascii=False)

    print(f"✅ Filled profile: {os.path.basename(path)}")

print("\n🎉 All profiles updated.")
