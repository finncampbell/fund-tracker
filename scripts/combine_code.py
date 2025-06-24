#!/usr/bin/env python3
import subprocess
import argparse
import os
from datetime import datetime

# --- CONFIGURATION ---
SKIP_WALK_DIRS     = {'.git', '__pycache__', 'node_modules'}
PATH_ONLY_DIRS     = {'data', 'fca-dashboard/data', 'docs/assets/data'}
CODE_EXTENSIONS    = {
    '.py', '.js', '.ts', '.java', '.cpp', '.c', '.h',
    '.go', '.rb', '.rs', '.sh', '.html', '.css',
    '.json', '.yml', '.yaml', '.md', '.tsx', '.jsx'
}
PATH_ONLY_EXTENSIONS = {'.csv'}

def run(cmd):
    return subprocess.check_output(cmd, text=True).splitlines()

def get_all_branches():
    lines = run(['git', 'branch', '-a'])
    branches = []
    for ln in lines:
        br = ln.strip().lstrip('* ').strip()
        if '->' in br:
            continue
        branches.append(br)
    return sorted(set(branches))

def list_files(branch):
    return run(['git', 'ls-tree', '-r', '--name-only', branch])

def show_file(branch, path):
    try:
        return subprocess.check_output(
            ['git', 'show', f'{branch}:{path}'],
            text=True
        )
    except subprocess.CalledProcessError:
        return None

def should_path_only(path):
    return any(path.startswith(d.rstrip('/') + '/') for d in PATH_ONLY_DIRS)

def is_code_file(path):
    return os.path.splitext(path)[1].lower() in CODE_EXTENSIONS

def is_path_only_ext(path):
    return os.path.splitext(path)[1].lower() in PATH_ONLY_EXTENSIONS

def main():
    parser = argparse.ArgumentParser(
        description="Combine all code across every branch into one file"
    )
    parser.add_argument(
        '-o','--out',
        default='combined_code.txt',
        help="Output file (default: combined_code.txt)"
    )
    args = parser.parse_args()

    branches = get_all_branches()
    with open(args.out, 'w', encoding='utf-8') as out:
        out.write(f"# Repo dump generated at {datetime.utcnow().isoformat()}Z\n\n")
        for branch in branches:
            out.write(f"# ===== BRANCH: {branch} =====\n\n")
            for path in list_files(branch):
                # Log but skip data dirs entirely
                if should_path_only(path):
                    out.write(f"# [DATA PATH] {path}\n\n")
                    continue

                ext = os.path.splitext(path)[1].lower()
                # CSVs (or other large file types) – only log path
                if is_path_only_ext(path):
                    out.write(f"# [PATH ONLY] {path}\n\n")
                    continue

                # Otherwise, dump header + content (or note non‑code)
                out.write(f"# ----- FILE: {path} -----\n")
                if is_code_file(path):
                    content = show_file(branch, path)
                    if content is not None:
                        out.write(content)
                    else:
                        out.write("# [Unable to read content]\n")
                else:
                    out.write("# [Logged only; non-code file]\n")
                out.write("\n")
            out.write("\n")
        out.write("# End of repo dump\n")

    print(f"✅ Combined code written to {args.out}")

if __name__ == '__main__':
    main()
