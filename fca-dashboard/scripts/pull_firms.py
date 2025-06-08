# fca-dashboard/scripts/pull_firms.py

import os
import requests
import json
from rate_limiter import RateLimiter

# Load API key from environment
API_KEY     = os.getenv("FCA_API_KEY")
if not API_KEY:
    raise EnvironmentError("FCA_API_KEY not set in environment")

# FCA endpoint
BASE_URL    = "https://api.fca.org.uk/firms"
HEADERS     = {"Authorization": f"Bearer {API_KEY}"}
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "../data/fca_firms.json")

limiter = RateLimiter()

def load_or_init_json(path, default):
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(default, f, indent=2)
        return default
    with open(path, "r") as f:
        return json.load(f)

def fetch_firm(frn):
    limiter.wait()
    url = f"{BASE_URL}/{frn}"
    r = requests.get(url, headers=HEADERS)
    if r.status_code == 200:
        return r.json()
    else:
        print(f"Failed to fetch FRN {frn}: HTTP {r.status_code}")
        return None

def main():
    # ensure JSON exists (initialises to empty list if missing)
    load_or_init_json(OUTPUT_PATH, [])

    # TODO: Replace with your full list of FRNs
    frns = [
      # e.g. "119293", "119348", "556677", ... 
      # put your test FRNs here
    ]

    # --- LIMIT TO FIRST 10 FOR TESTING ---
    frns = frns[:10]

    results = []
    for frn in frns:
        data = fetch_firm(frn)
        if data:
            results.append(data)

    with open(OUTPUT_PATH, "w") as f:
        json.dump(results, f, indent=2)

    print(f"[TEST MODE] Wrote {len(results)} firms (max 10) to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
