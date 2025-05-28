#!/usr/bin/env python3
"""
test_api_key.py

– Verifies that the CH_API_KEY env var is set
– Ensures `requests` is installed (installs via pip if missing)
– Makes a simple request to the Companies House Search API
– Exits 0 on success (status 200), non-zero otherwise
"""

import os
import sys
import subprocess

# Ensure requests is installed
try:
    import requests
except ImportError:
    print("`requests` module not found. Installing via pip...", file=sys.stderr)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
    import requests

API_URL = "https://api.company-information.service.gov.uk/search/companies"

def main():
    key = os.getenv("CH_API_KEY")
    if not key:
        print("ERROR: CH_API_KEY environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    try:
        resp = requests.get(
            API_URL,
            auth=(key, ""),
            params={"q": "test", "items_per_page": 1},
            timeout=10
        )
    except Exception as e:
        print(f"ERROR: Request failed: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"HTTP {resp.status_code}")
    if resp.status_code == 200:
        try:
            data = resp.json()
            count = len(data.get("items", []))
            print(f"SUCCESS: Retrieved {count} item(s).")
        except Exception:
            print("SUCCESS: Status 200, but failed to parse JSON.")
        sys.exit(0)
    else:
        print(f"ERROR: Response body:\n{resp.text}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
