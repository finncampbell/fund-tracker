#!/usr/bin/env python3
import os
import argparse

# Adjust extensions as needed
CODE_EXTENSIONS = {
    '.py', '.js', '.ts', '.java', '.cpp', '.c', '.h',
    '.go', '.rb', '.rs', '.sh', '.html', '.css', '.tsx', '.jsx'
}

def should_include(filename):
    _, ext = os.path.splitext(filename)
    return ext.lower() in CODE_EXTENSIONS

def combine_repo_code(root_dir, output_path):
    with open(output_path, 'w', encoding='utf-8') as out_file:
        for dirpath, dirnames, filenames in os.walk(root_dir):
            # Skip unwanted folders
            dirnames[:] = [
                d for d in dirnames
                if d not in ('.git', '__pycache__', 'node_modules')
            ]
            for fname in sorted(filenames):
                if should_include(fname):
                    full_path = os.path.join(dirpath, fname)
                    rel_path = os.path.relpath(full_path, root_dir)
                    out_file.write(f"\n\n# ===== File: {rel_path} =====\n\n")
                    try:
                        with open(full_path, 'r', encoding='utf-8') as in_file:
                            out_file.write(in_file.read())
                    except Exception as e:
                        out_file.write(f"# [Could not read file: {e}]\n")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Combine all code files in a repo into one file"
    )
    parser.add_argument(
        'root',
        nargs='?',
        default='.',
        help="Repo root (default: current dir)"
    )
    parser.add_argument(
        '-o', '--output',
        default='combined_code.txt',
        help="Output filename (default: combined_code.txt)"
    )
    args = parser.parse_args()
    combine_repo_code(args.root, args.output)
    print(f"Combined into {args.output}")
