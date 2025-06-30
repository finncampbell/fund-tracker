#!/usr/bin/env python3
"""
scripts/merge_fca_individuals.py

Merge all chunked FCA individuals JSON files in a directory into one consolidated JSON.

Usage:
    merge_fca_individuals.py <chunks_dir> <output_json>
"""
import os
import sys
import json

def merge_chunks(chunk_dir, out_path):
    merged = {}

    # Load each .json file in chunk_dir
    for fname in os.listdir(chunk_dir):
        if not fname.endswith('.json'):
            continue
        path = os.path.join(chunk_dir, fname)
        print(f"⏳ Loading {path}")
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not isinstance(data, dict):
                print(f"⚠️  Skipping {fname}: not a JSON object")
                continue
            # Override or add each FRN entry
            merged.update(data)
        except Exception as e:
            print(f"❌ Failed to load {fname}: {e}")

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # Write the merged result
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
