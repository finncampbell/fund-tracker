#!/usr/bin/env python3
"""
Merge FCA slices + compressed CF into each FRN profile.
"""
import os
import json
import glob
import zipfile

# Paths
RAW       = os.path.abspath('fca-dashboard/data')
PROF      = os.path.abspath('docs/fca-dashboard/data/frn')
CF_ZIP    = os.path.abspath('docs/fca-dashboard/data/fca_cf.zip')

# 1. Load firm slices
main_slice    = json.load(open(os.path.join(RAW, 'fca_main.json')))
names_slice   = json.load(open(os.path.join(RAW, 'fca_names.json')))
ars_slice     = json.load(open(os.path.join(RAW, 'fca_ars.json')))
indivs_slice  = json.load(open(os.path.join(RAW, 'fca_individuals_by_firm.json')))
persons_slice = json.load(open(os.path.join(RAW, 'fca_persons.json')))

# 2. Load the compressed CF map
print(f"📦 Loading CF from {CF_ZIP}")
with zipfile.ZipFile(CF_ZIP, 'r') as z:
    with z.open('fca_cf.json') as f:
        cf_by_irn = json.load(f)
print(f"🔄 CF loaded for {len(cf_by_irn)} IRNs")

# 3. Inject into each profile stub
for path in glob.glob(os.path.join(PROF, '*.json')):
    profile = json.load(open(path, encoding='utf-8'))
    frn = str(profile.get('frn'))

    # Merge in slices
    profile['metadata']           = main_slice.get(frn, {})
    profile['trading_names']      = names_slice.get(frn, [])
    profile['appointed_reps']     = ars_slice.get(frn, [])

    indivs = indivs_slice.get(frn, [])
    profile['individuals']        = indivs

    # Build person_records
    person_recs = {
        irn: persons_slice[irn]
        for rec in indivs
        if (irn := str(rec.get('IRN'))) in persons_slice
    }
    profile['person_records']      = person_recs

    # Controlled functions
    profile['controlled_functions'] = {
        irn: cf_by_irn.get(irn, [])
        for irn in person_recs
    }

    # Write back
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(profile, f, indent=2, ensure_ascii=False)
    print(f"✅ Updated {os.path.basename(path)}")

print("🎉 All profiles rebuilt.")
