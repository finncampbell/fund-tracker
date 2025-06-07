#!/usr/bin/env python3
import os
import argparse
import subprocess
import tempfile
import shutil

# Adjust extensions as needed
CODE_EXTENSIONS = {
    '.py', '.js', '.ts', '.java', '.cpp', '.c', '.h',
    '.go', '.rb', '.rs', '.sh', '.html', '.css', '.tsx', '.jsx'
}

# Default directories to skip
SKIP_DIRS = {'.git', '__pycache__', 'node_modules', 'docs/assets/data', 'data-branch', 'data'}

# Extensions for which we only want to record paths, not content
PATH_ONLY_EXTENSIONS = {'.csv'}


def should_include(filename):
    _, ext = os.path.splitext(filename)
    return ext.lower() in CODE_EXTENSIONS


def is_path_only(filename):
    _, ext = os.path.splitext(filename)
    return ext.lower() in PATH_ONLY_EXTENSIONS


def combine_repo_code(root_dir, output_path, out_file=None):
    # If an open file handle is provided, use it, else open a new one
    manage_file = False
    if out_file is None:
        out_file = open(output_path, 'w', encoding='utf-8')
        manage_file = True

    try:
        for dirpath, dirnames, filenames in os.walk(root_dir):
            # Skip unwanted folders
            dirnames[:] = [
                d for d in dirnames
                if d not in SKIP_DIRS
            ]
            for fname in sorted(filenames):
                full_path = os.path.join(dirpath, fname)
                rel_path = os.path.relpath(full_path, root_dir)
                if should_include(fname):
                    out_file.write(f"\n\n# ===== File: {rel_path} =====\n\n")
                    try:
                        with open(full_path, 'r', encoding='utf-8') as in_file:
                            out_file.write(in_file.read())
                    except Exception as e:
                        out_file.write(f"# [Could not read file: {e}]\n")
                elif is_path_only(fname):
                    # Write only the CSV location, not its content
                    out_file.write(f"\n# ===== CSV Path: {rel_path} =====\n")
    finally:
        if manage_file:
            out_file.close()


def process_local(root, output):
    combine_repo_code(root, output)
    print(f"Combined into {output}")


def process_remote(repo_url, output):
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"Cloning {repo_url} into {tmpdir} (all branches)...")
        try:
            subprocess.check_call(['git', 'clone', '--no-single-branch', repo_url, tmpdir])
            cwd = os.getcwd()
            os.chdir(tmpdir)
            branches = subprocess.check_output(['git', 'branch', '-r']).decode().splitlines()
            branches = [b.strip() for b in branches if '->' not in b]
            # Open output in append mode
            with open(output, 'w', encoding='utf-8') as out_file:
                for remote_branch in branches:
                    branch_name = remote_branch.replace('origin/', '')
                    subprocess.check_call(['git', 'checkout', branch_name])
                    out_file.write(f"\n\n# ===== Branch: {branch_name} =====\n")
                    combine_repo_code(tmpdir, output, out_file)
            os.chdir(cwd)
        except subprocess.CalledProcessError as e:
            print(f"Error processing repository: {e}")
            return
    print(f"Combined into {output}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Combine all code files in a repo into one file"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--root',
        help="Local repo root to scan"
    )
    group.add_argument(
        '--repo',
        help="GitHub repo URL to clone and scan (e.g., https://github.com/user/repo.git)"
    )
    parser.add_argument(
        '-o', '--output',
        default='combined_code.txt',
        help="Output filename (default: combined_code.txt)"
    )
    args = parser.parse_args()

    if args.repo:
        process_remote(args.repo, args.output)
    else:
        root = args.root or '.'
        process_local(root, args.output)
