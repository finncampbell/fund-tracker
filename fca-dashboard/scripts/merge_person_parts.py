#!/usr/bin/env python3
"""
scripts/merge_person_parts.py

Merge per-shard JSON outputs into the final fca_persons.json.
"""
import os
import json
import glob
import argparse

parser = argparse.ArgumentParser(
    description='Merge partitioned individual-record JSONs'
)
parser.add_argument('input_dir', help='Directory containing fca_persons_partN.json')
parser.add_argument('output_file', help='Path for merged fca_persons.json')
args = parser.parse_args()

all_data = {}
for part_path in sorted(glob.glob(os.path.join(args.input_dir, 'fca_persons_part*.json'))):
    with open(part_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    all_data.update(data)

os.makedirs(os.path.dirname(args.output_file), exist_ok=True)
with open(args.output_file, 'w', encoding='utf-8') as out:
    json.dump(all_data, out, indent=2)
print(f"✅ Merged {len(all_data)} records into {args.output_file}")
