#!/usr/bin/env python3
"""
scripts/merge_cf_shards.py

Merge all per‐shard fca_cf_part<N>.json files in a folder into one fca_cf.json.

Usage:
  python3 fca-dashboard/scripts/merge_cf_shards.py <shard-dir> <output-path>
"""
import sys, os, json, glob

def main():
    if len(sys.argv) != 3:
        print("Usage: merge_cf_shards.py <shard-dir> <output-path>")
        sys.exit(1)

    shard_dir, out_path = sys.argv[1], sys.argv[2]

    merged = {}
    pattern = os.path.join(shard_dir, "fca_cf_part*.json")
    for fn in sorted(glob.glob(pattern)):
        with open(fn, 'r', encoding='utf-8') as f:
            part = json.load(f)
        merged.update(part)
        print(f"Merged {len(part)} IRNs from {os.path.basename(fn)}")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Done: wrote {len(merged)} IRNs to {out_path}")

if __name__ == '__main__':
    main()
