#!/usr/bin/env python3
"""
scripts/merge_cf_parts.py

Merge all per‐shard fca_cf_part<k>.json files in a folder into one fca_cf.json.
Usage:
  python3 fca-dashboard/scripts/merge_cf_parts.py <shard-dir> <output-file>
"""
import sys, os, json, glob

def main():
    if len(sys.argv) != 3:
        print("Usage: merge_cf_parts.py <shard-dir> <output-path>")
        sys.exit(1)

    shard_dir, out_path = sys.argv[1], sys.argv[2]

    merged = {}
    pattern = os.path.join(shard_dir, "fca_cf_part*.json")
    for fn in sorted(glob.glob(pattern)):
        with open(fn, 'r', encoding='utf-8') as f:
            part = json.load(f)
        # each part is IRN->list; later parts override if duplicates
        merged.update(part)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)

    print(f"✅ Merged {len(merged)} IRNs into {out_path}")

if __name__ == '__main__':
    main()
