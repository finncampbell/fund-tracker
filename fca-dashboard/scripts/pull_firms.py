import requests
import json
from rate_limiter import RateLimiter

API_KEY = "your_fca_api_key_here"
BASE_URL = "https://api.fca.org.uk/firms"  # Example; replace with actual endpoint

HEADERS = {"Authorization": f"Bearer {API_KEY}"}
limiter = RateLimiter()

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
    # Replace with your logic to fetch a list of FRNs
    frns = [...]  # e.g., from a file, existing dataset, or discovery
    output = []

    for frn in frns:
        data = fetch_firm(frn)
        if data:
            output.append(data)

    with open("../data/fca_firms.json", "w") as f:
        json.dump(output, f, indent=2)

if __name__ == "__main__":
    main()

