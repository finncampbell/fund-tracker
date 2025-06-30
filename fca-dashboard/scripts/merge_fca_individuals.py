#!/usr/bin/env python3
"""
scripts/merge_fca_individuals.py

Takes multiple partial JSON outputs (each a dict mapping FRN→list)
and merges them into one consolidated JSON file.
"""
import sys
import os
import json

def merge_chunks(chunk_dir, out_path):
    merged = {}
    for fname in os.listdir(chunk_dir):
        if not fname.endswith('.json'):
            continue
        full_path = os.path.join(chunk_dir, fname)
        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                part = json.load(f)
            if not isinstance(part, dict):
                print(f"⚠️  Skipping non-dict chunk {fname}")
                continue
            for frn, lst in part.items():
                # override any previous entry for this FRN
                merged[frn] = lst
        except Exception as e:
            print(f"❌ Failed to load {fname}: {e}")

    # write the merged result
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    print(f"✅ Merged {len(merged)} FRNs into {out_path}")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: merge_fca_individuals.py <chunks_dir> <output_json>")
        sys.exit(1)
    chunks_directory = sys.argv[1]
    output_file      = sys.argv[2]
    merge_chunks(chunks_directory, output_file)
