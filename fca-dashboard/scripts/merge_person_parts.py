#!/usr/bin/env python3
"""
scripts/merge_person_parts.py

Merge per-shard JSON outputs into the final fca_persons.json,
preserving existing entries and only adding missing IRNs.
"""
import os
import json
import glob
import argparse

parser = argparse.ArgumentParser(
    description='Merge partitioned individual-record JSONs without overwriting'
)
parser.add_argument('input_dir', help='Directory with fca_persons_part*.json')
parser.add_argument('output_file', help='Path for merged fca_persons.json')
args = parser.parse_args()

# 1) Load or initialize the existing store
if os.path.exists(args.output_file):
    with open(args.output_file, 'r', encoding='utf-8') as f:
        merged = json.load(f)
    if not isinstance(merged, dict):
        print(f"⚠️ Existing file not dict; starting fresh.")
        merged = {}
else:
    merged = {}

# 2) Iterate shards, add only new IRNs
pattern = os.path.join(args.input_dir, 'fca_persons_part*.json')
for part_path in sorted(glob.glob(pattern)):
    with open(part_path, 'r', encoding='utf-8') as f:
        shard_data = json.load(f)
    for irn, rec in shard_data.items():
        if irn not in merged:
            merged[irn] = rec

# 3) Write merged output
os.makedirs(os.path.dirname(args.output_file), exist_ok=True)
with open(args.output_file, 'w', encoding='utf-8') as out:
    json.dump(merged, out, indent=2, ensure_ascii=False)

print(f"✅ Merged shards: {len(merged)} total records into {args.output_file}")
