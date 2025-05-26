# rate_limiter.py

import json
import time
import os
from collections import deque
from filelock import FileLock
import requests  # assuming you use requests for HTTP calls

# Constants
RATE_LIMIT       = 600
CALL_BUFFER      = 50
WINDOW_SECONDS   = 5 * 60
STATE_FILE       = os.environ.get("RATE_LIMIT_STATE_FILE", "assets/logs/rate_limit.json")
LOCK_FILE        = STATE_FILE + ".lock"


def _load_state():
    if not os.path.exists(STATE_FILE):
        return deque()
    with open(STATE_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return deque(data)


def _save_state(timestamps):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(list(timestamps), f)


def _prune(timestamps):
    cutoff = time.time() - WINDOW_SECONDS
    while timestamps and timestamps[0] < cutoff:
        timestamps.popleft()


def enforce_rate_limit(response=None):
    """
    Call this before making an API request. Optionally pass in a
    `requests.Response` if handling a retry after a 429.
    """
    with FileLock(LOCK_FILE):
        # Load and prune stale entries
        timestamps = _load_state()
        _prune(timestamps)

        # If we just got a 429, back off per Retry-After (or default 20s)
        if response is not None and response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            wait = int(retry_after) if retry_after and retry_after.isdigit() else 20
            time.sleep(wait)
            _prune(timestamps)

        # Compute available slots after preserving buffer
        used = len(timestamps)
        free_slots = RATE_LIMIT - used - CALL_BUFFER
        if free_slots <= 0:
            # Sleep only as long as needed for the oldest timestamp to expire
            oldest = timestamps[0]
            to_sleep = WINDOW_SECONDS - (time.time() - oldest)
            if to_sleep > 0:
                time.sleep(to_sleep)
            _prune(timestamps)

        # Record this call and immediately persist
        timestamps.append(time.time())
        _save_state(timestamps)


# Example wrapper for an API call
def make_api_call(url, **kwargs):
    """
    Wrap your actual HTTP calls with this function to ensure
    rate-limit enforcement and 429 handling.
    """
    response = None
    for attempt in range(1, 4):
        enforce_rate_limit(response)
        response = requests.get(url, **kwargs)
        if response.status_code == 429:
            # Let enforce_rate_limit handle the back-off next iteration
            continue
        if 500 <= response.status_code < 600:
            time.sleep(5)
            continue
        break  # success or non-retryable status

    return response
