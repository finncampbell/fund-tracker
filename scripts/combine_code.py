#!/usr/bin/env python3
import subprocess
import argparse
import os
import sys
from datetime import datetime

# --- CONFIGURATION ---
SKIP_WALK_DIRS       = {'.git', '__pycache__', 'node_modules'}
PATH_ONLY_DIRS       = {
    'data',
    'fca-dashboard/data',
    'docs/assets/data',
    'assets/data',
    'fca-dashboard/assets/data'
}
CODE_EXTENSIONS      = {
    '.py', '.js', '.ts', '.java', '.cpp', '.c', '.h',
    '.go', '.rb', '.rs', '.sh', '.html', '.css',
    '.yml', '.yaml', '.md', '.tsx', '.jsx'
}
PATH_ONLY_EXTENSIONS = {'.csv', '.json'}

def run(cmd):
    return subprocess.check_output(cmd, text=True).splitlines()

def fetch_all():
    print("🔄 Fetching all remotes...", file=sys.stderr)
    run(['git', 'fetch', '--all', '--prune'])

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

def build_tree(paths):
    tree = {}
    for p in paths:
        parts = p.split('/')
        node = tree
        for part in parts:
            node = node.setdefault(part, {})
    return tree

def print_tree(node, prefix='', branch='', current_path='', out=None):
    items = sorted(node.items())
    for idx, (name, child) in enumerate(items):
        is_last = (idx == len(items) - 1)
        connector = '└── ' if is_last else '├── '
        line = f"{prefix}{connector}{name}"
        full_path = os.path.join(current_path, name) if current_path else name
        ext = os.path.splitext(name)[1].lower()

        if child:
            print(line, file=out)
            new_prefix = prefix + ('    ' if is_last else '│   ')
            print_tree(child, new_prefix, branch, full_path, out)
        else:
            # data-only directories or explicit path-only extensions
            if any(full_path.startswith(d.rstrip('/') + os.sep) for d in PATH_ONLY_DIRS) \
               or ext in PATH_ONLY_EXTENSIONS:
                print(f"{line}  [PATH ONLY]", file=out)

            # code files only
            elif ext in CODE_EXTENSIONS:
                print(line, file=out)
                content = show_file(branch, full_path)
                if content is not None:
                    for cl in content.splitlines():
                        print(f"{prefix}    {cl}", file=out)
                else:
                    print(f"{prefix}    [Unable to read '{full_path}']", file=out)

            else:
                print(f"{line}  [SKIPPED NON-CODE]", file=out)

def main():
    parser = argparse.ArgumentParser(
        description="Map every file and directory, branch by branch, as an ASCII tree"
    )
    parser.add_argument(
        '-o', '--out',
        default='repo_tree.txt',
        help="Output file (default: repo_tree.txt)"
    )
    args = parser.parse_args()

    fetch_all()
    branches = get_all_branches()

    with open(args.out, 'w', encoding='utf-8') as out:
        out.write(f"# Repo tree generated at {datetime.utcnow().isoformat()}Z\n\n")
        for branch in branches:
            out.write(f"# ===== BRANCH: {branch} =====\n")
            files = list_files(branch)
            tree = build_tree(files)
            print_tree(tree, prefix='', branch=branch, current_path='', out=out)
            out.write("\n")
        out.write("# End of repo tree\n")

    print(f"✅ Repo tree written to {args.out}")

if __name__ == '__main__':
    main()
