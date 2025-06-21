#!/usr/bin/env python3
import os, requests, sys

API_EMAIL = os.getenv("FCA_API_EMAIL")
API_KEY   = os.getenv("FCA_API_KEY")
if not API_EMAIL or not API_KEY:
    print("❌ Missing FCA_API_EMAIL or FCA_API_KEY")
    sys.exit(1)

url = "https://register.fca.org.uk/services/V0.1/Firm/119293"  # use a known FRN
headers = {
    "Accept":       "application/json",
    "X-AUTH-EMAIL": API_EMAIL,
    "X-AUTH-KEY":   API_KEY,
}

print(f"Testing connection to {url!r} …")
try:
    r = requests.get(url, headers=headers, timeout=10)
    print("→ HTTP", r.status_code)
    # Print just the top‑level keys so we don’t dump megabytes
    if r.status_code == 200 and r.headers.get("Content-Type","").startswith("application/json"):
        data = r.json()
        print("→ JSON keys:", list(data.keys()))
        sys.exit(0)
    else:
        print("❌ Unexpected response:", r.text[:200])
        sys.exit(1)
except Exception as e:
    print("❌ Exception:", e)
    sys.exit(1)
