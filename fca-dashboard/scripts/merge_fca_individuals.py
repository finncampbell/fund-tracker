#!/usr/bin/env python3
"""
scripts/merge_fca_individuals.py

Merge all chunked fca_individuals_by_firm.json artifacts into a single
fca-dashboard/data/fca_individuals_by_firm.json.
"""
import os
import json
import glob

# where the final file should live:
OUT_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '../fca-dashboard/data/fca_individuals_by_firm.json'
))

def main():
    merged = {}
    # assume downloaded artifacts are under ./chunks/individuals-chunk-*/fca_individuals_by_firm.json
    for path in glob.glob('chunks/individuals-chunk-*/fca_individuals_by_firm.json'):
        print(f"⏳ Loading {path}")
        with open(path, 'r', encoding='utf-8') as f:
            partial = json.load(f)
        merged.update(partial)

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    print(f"✅ Merged {len(merged)} FRNs into {OUT_PATH}")

if __name__ == '__main__':
    main()
