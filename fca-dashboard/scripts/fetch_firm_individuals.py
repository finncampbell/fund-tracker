#!/usr/bin/env python3
"""
scripts/merge_fca_individuals.py

Takes multiple partial JSON outputs (each a dict mapping FRN→list)
and merges them into one consolidated JSON file.
"""
import sys, os, json

def merge_chunks(chunk_dir, out_path):
    merged = {}
    for fname in os.listdir(chunk_dir):
        if not fname.endswith('.json'):
            continue
        full = os.path.join(chunk_dir, fname)
        try:
            with open(full, 'r', encoding='utf-8') as f:
                part = json.load(f)
            if not isinstance(part, dict):
                print(f"⚠️  Skipping non-dict chunk {fname}")
                continue
            for frn, lst in part.items():
                # Overwrite or append? we'll just override with latest chunk
                merged[frn] = lst
        except Exception as e:
            print(f"❌ Failed to load {fname}: {e}")

    # write out
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    print(f"✅ Merged {len(merged)} FRNs into {out_path}")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: merge_fca_individuals.py <chunks_dir> <output_json>")
        sys.exit(1)
    merge_chunks(sys.argv[1], sys.argv[2])
