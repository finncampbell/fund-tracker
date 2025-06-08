import requests
import json
import time
from datetime import datetime, timedelta
from rate_limiter import RateLimiter
import os

API_KEY = "your_fca_api_key_here"
BASE_URL = "https://api.fca.org.uk/individuals"  # Update as needed
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

CACHE_PATH = "../data/fca_individuals.json"
REFRESH_AFTER_DAYS = 7

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

def load_cache():
    if not os.path.exists(CACHE_PATH):
        return {}
    with open(CACHE_PATH, "r") as f:
        data = json.load(f)
    return {item["person_id"]: item for item in data}

def save_cache(data_dict):
    with open(CACHE_PATH, "w") as f:
        json.dump(list(data_dict.values()), f, indent=2)

def main():
    person_ids = [...]  # Replace with full known list
    cache = load_cache()
    now = datetime.utcnow()
    updated = 0

    for pid in person_ids:
        entry = cache.get(pid)
        last_seen = datetime.strptime(entry["last_seen"], "%Y-%m-%dT%H:%M:%S") if entry else None
        needs_update = not entry or (now - last_seen).days >= REFRESH_AFTER_DAYS

        if needs_update:
            print(f"Updating person {pid}")
            data = fetch_person(pid)
            if data:
                data["person_id"] = pid
                data["last_seen"] = now.strftime("%Y-%m-%dT%H:%M:%S")
                cache[pid] = data
                updated += 1

    print(f"Updated {updated} individuals.")
    save_cache(cache)

if __name__ == "__main__":
    main()
