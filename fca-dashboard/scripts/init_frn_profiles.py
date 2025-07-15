#!/usr/bin/env python3
"""
scripts/init_frn_profiles.py

Initialize one empty profile JSON per FRN from the seed list:
docs/fca-dashboard/data/all_frns_with_names.json → docs/fca-dashboard/data/frn/
Filename format: <CleanName>.<FRN>.json
"""
import os
import json
import re
import sys

# Paths
BASE_DIR = os.path.join('docs', 'fca-dashboard', 'data')
SEED     = os.path.join(BASE_DIR, 'all_frns_with_names.json')
OUT_DIR  = os.path.join(BASE_DIR, 'frn')

# Ensure output folder exists
os.makedirs(OUT_DIR, exist_ok=True)

# Load seed list
try:
    with open(SEED, 'r', encoding='utf-8') as f:
        entries = json.load(f)
except FileNotFoundError:
    print(f"❌ Seed file not found: {SEED}", file=sys.stderr)
    sys.exit(1)

# Helper to make safe filenames
def clean_name(s: str) -> str:
    s = re.sub(r'\s+', '_', s.strip())
    return re.sub(r'[^A-Za-z0-9_]', '', s) or 'Unnamed'

# Write one skeleton per FRN
for e in entries:
    frn = str(e.get('frn') or e.get('FRN') or '')
    name = e.get('name') or e.get('Name') or ''
    if not frn:
        print(f"⚠️  Skipping entry without FRN: {e}", file=sys.stderr)
        continue

    filename = f"{clean_name(name)}.{frn}.json"
    outpath  = os.path.join(OUT_DIR, filename)

    profile = {
        "frn": int(frn),
        "name": name,
        # placeholders—will fill these later
        "trading_names": [],
        "appointed_reps": [],
        "individuals": [],
        "person_metadata": {},
        "controlled_functions": {}
    }

    with open(outpath, 'w', encoding='utf-8') as f:
        json.dump(profile, f, indent=2, ensure_ascii=False)

    print(f"📝 Created profile skeleton: {filename}")

print(f"\n✅ Initialized {len(entries)} profile files under {OUT_DIR}")
