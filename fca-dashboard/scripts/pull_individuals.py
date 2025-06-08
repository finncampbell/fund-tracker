import requests
import json
import os
from datetime import datetime, timedelta
from rate_limiter import RateLimiter

API_KEY           = "your_fca_api_key_here"
BASE_URL          = "https://api.fca.org.uk/individuals"  # adjust as needed
HEADERS           = {"Authorization": f"Bearer {API_KEY}"}
CACHE_PATH        = os.path.join(os.path.dirname(__file__), "../data/fca_individuals.json")
REFRESH_AFTER_DAYS = 7
limiter           = RateLimiter()

def load_or_init_json(path, default):
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(default, f, indent=2)
        return default
    with open(path, "r") as f:
        return json.load(f)

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
    cache_list = load_or_init_json(CACHE_PATH, [])
    cache = {item["person_id"]: item for item in cache_list}

    # Replace with full list of person IDs you want to track
    person_ids = [...]  
    now = datetime.utcnow()
    updated = 0

    for pid in person_ids:
        entry = cache.get(pid)
        last_seen = datetime.strptime(entry["last_seen"], "%Y-%m-%dT%H:%M:%S") if entry else None
        needs_update = (not entry) or ((now - last_seen).days >= REFRESH_AFTER_DAYS)

        if needs_update:
            print(f"Updating person {pid}")
            data = fetch_person(pid)
            if data:
                data["person_id"] = pid
                data["last_seen"] = now.strftime("%Y-%m-%dT%H:%M:%S")
                cache[pid] = data
                updated += 1

    out_list = list(cache.values())
    with open(CACHE_PATH, "w") as f:
        json.dump(out_list, f, indent=2)

    print(f"Updated {updated} individuals. Cache size now {len(out_list)}.")

if __name__ == "__main__":
    main()
