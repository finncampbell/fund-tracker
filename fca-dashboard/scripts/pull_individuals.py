import requests
import json
from rate_limiter import RateLimiter

API_KEY = "your_fca_api_key_here"
BASE_URL = "https://api.fca.org.uk/individuals"  # Example; replace as needed

HEADERS = {"Authorization": f"Bearer {API_KEY}"}
limiter = RateLimiter()

def fetch_person(person_id):
    limiter.wait()
    url = f"{BASE_URL}/{person_id}"
    r = requests.get(url, headers=HEADERS)
    if r.status_code == 200:
        return r.json()
    else:
        print(f"Failed to fetch person {person_id}: {r.status_code}")
        return None

def main():
    # Replace with actual list of new or stale person IDs
    person_ids = [...]  # from cached dataset or fresh firm scrape
    output = []

    for pid in person_ids:
        data = fetch_person(pid)
        if data:
            output.append(data)

    with open("../data/fca_individuals.json", "w") as f:
        json.dump(output, f, indent=2)

if __name__ == "__main__":
    main()

