#!/usr/bin/env python3
"""
Simple smoke-test: fetch a single known FRN to verify API access.
"""
import os
import sys
import requests

FRN = "401805"
URL = f"https://register.fca.org.uk/services/V0.1/Firm/{FRN}"
API_KEY   = os.getenv("FCA_API_KEY")
API_EMAIL = os.getenv("FCA_API_EMAIL")

if not API_KEY or not API_EMAIL:
    print("❌ Please set FCA_API_KEY and FCA_API_EMAIL environment variables.")
    sys.exit(1)

headers = {
    "x-auth-key":   API_KEY,
    "x-auth-email": API_EMAIL,
    "Content-Type": "application/json"
}

print(f"🔍 Fetching FRN {FRN} from {URL}")
resp = requests.get(URL, headers=headers, timeout=10)
print(f"➡️  HTTP {resp.status_code} {resp.reason}")

try:
    body = resp.json()
except ValueError:
    print("❌ Response was not valid JSON:")
    print(resp.text)
    sys.exit(1)

if resp.status_code == 200:
    print("✅ Success! Here’s the top‐level keys in the JSON:")
    print("   " + ", ".join(body.keys()))
    data = body.get("Data", [])
    if data:
        print("✅ ‘Data’ array length:", len(data))
        print("Sample entry fields:", ", ".join(data[0].keys()))
    else:
        print("⚠️ ‘Data’ array is empty.")
    sys.exit(0)

# Non-200
print("❌ Non-200 response body:")
print(body)
sys.exit(1)
