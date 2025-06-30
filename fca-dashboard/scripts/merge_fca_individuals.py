#!/usr/bin/env python3
"""
scripts/merge_fca_individuals.py

Merge all chunked fca_individuals_by_firm.json artifacts into a single
fca-dashboard/data/fca_individuals_by_firm.json.
Takes multiple partial JSON outputs (each a dict mapping FRN→list)
and merges them into one consolidated JSON file.
"""
import sys
import os
import json
import glob

def merge_chunks(chunk_dir, out_path):
    merged = {}

    # look recursively under each chunk directory for the JSON files
    pattern = os.path.join(chunk_dir, 'individuals-chunk-*', '**', 'fca_individuals_by_firm.json')
    for path in glob.glob(pattern, recursive=True):
        print(f"⏳ Loading {path}")
        try:
            with open(path, 'r', encoding='utf-8') as f:
                partial = json.load(f)
        except Exception as e:
            print(f"❌ Failed to load {path}: {e}")
            continue

        if not isinstance(partial, dict):
            print(f"⚠️  Skipping non-dict file {path}")
            continue

        # merge/override entries
        for frn, lst in partial.items():
            merged[frn] = lst

    # write out the final merged JSON
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)

    print(f"✅ Merged {len(merged)} FRNs into {out_path}")


def main():
    if len(sys.argv) != 3:
        print("Usage: merge_fca_individuals.py <chunks_dir> <output_json>")
        sys.exit(1)

    chunk_dir = sys.argv[1]
    out_path  = sys.argv[2]
    merge_chunks(chunk_dir, out_path)


if __name__ == '__main__':
    main()
