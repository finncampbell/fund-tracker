#!/usr/bin/env python3
"""
scripts/merge_fca_individuals.py

After all parallel chunks have run, this script scans the downloaded
artifacts under a “chunks/” directory, merges each partial JSON
(FRN → [individuals]) into one big dict, and writes the final
fca-dashboard/data/fca_individuals_by_firm.json.
"""

import sys
import os
import json
import glob

def merge_chunks(chunk_dir, out_path):
    merged = {}

    # Build a glob pattern to find every chunk’s JSON file
    pattern = os.path.join(
      chunk_dir,
      'individuals-chunk-*',
      '**',
      'fca_individuals_by_firm.json'
    )

    # Iterate all matches (recursive glob)
    for path in glob.glob(pattern, recursive=True):
        print(f"⏳ Loading chunk file {path}")
        try:
            with open(path, 'r', encoding='utf-8') as f:
                partial = json.load(f)
        except Exception as e:
            print(f"❌ Could not load {path}: {e}")
            continue

        if not isinstance(partial, dict):
            print(f"⚠️  Skipping non-dict file {path}")
            continue

        # Overwrite or add each FRN entry
        for frn, lst in partial.items():
            merged[frn] = lst

    # Ensure output directory exists
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # Write the merged result
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)

    print(f"✅ Merged {len(merged)} FRNs into {out_path}")

def main():
    # Basic usage guard
    if len(sys.argv) != 3:
        print("Usage: merge_fca_individuals.py <chunks_dir> <output_json>")
        sys.exit(1)

    chunk_dir = sys.argv[1]
    out_path  = sys.argv[2]
    merge_chunks(chunk_dir, out_path)

if __name__ == '__main__':
    main()
