import requests
import json
import os
from rate_limiter import RateLimiter

API_KEY       = "your_fca_api_key_here"
BASE_URL      = "https://api.fca.org.uk/firms"   # replace with actual
HEADERS       = {"Authorization": f"Bearer {API_KEY}"}
OUTPUT_PATH   = os.path.join(os.path.dirname(__file__), "../data/fca_firms.json")
limiter       = RateLimiter()

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
        print(f"Failed to fetch FRN {frn}: {r.status_code}")
        return None

def main():
    # ensure output exists (starts empty list)
    firms = load_or_init_json(OUTPUT_PATH, [])

    # Replace this with your own FRN-list source
    frns = [...]  

    result = []
    for frn in frns:
        data = fetch_firm(frn)
        if data:
            result.append(data)

    with open(OUTPUT_PATH, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Wrote {len(result)} firms to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
