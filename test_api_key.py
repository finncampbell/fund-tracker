#!/usr/bin/env python3
"""
test_api_key.py

– Verifies CH_API_KEY is set
– Ensures `requests` is installed
– Makes a sanity check request to a known Companies House endpoint
– Dumps URL, status, headers, and body
– Exits 0 on 2xx, non-zero otherwise
"""

import os
import sys
import subprocess

# ─── Ensure requests is installed ────────────────────────────────────────────────
try:
    import requests
except ImportError:
    print("`requests` not found; installing…", file=sys.stderr)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
    import requests

# ─── Configuration ───────────────────────────────────────────────────────────────
API_KEY = os.getenv("CH_API_KEY")
if not API_KEY:
    print("ERROR: CH_API_KEY is not set.", file=sys.stderr)
    sys.exit(1)

# Use a known company number (e.g. Tate & Lyle PLC: 00000006)
TEST_COMPANY = "00000006"
URL = f"https://api.company-information.service.gov.uk/company/{TEST_COMPANY}"

def main():
    try:
        resp = requests.get(URL, auth=(API_KEY, ""), timeout=10)
    except Exception as e:
        print(f"ERROR: Request exception: {e}", file=sys.stderr)
        sys.exit(1)

    # Echo request/response details
    print("→ Request URL:", resp.request.method, resp.request.url)
    print("→ Status Code:", resp.status_code)
    print("→ Response Headers:")
    for k, v in resp.headers.items():
        print(f"   {k}: {v}")
    print("→ Response Body:")
    print(resp.text)

    # Exit success on any 2xx
    if 200 <= resp.status_code < 300:
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
