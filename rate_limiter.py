# rate_limiter.py

import json
import time
import os
from collections import deque
from filelock import FileLock
import requests

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
    Call before each HTTP request. If `response` is a 429, backs off per Retry-After.
    """
    with FileLock(LOCK_FILE):
        timestamps = _load_state()
        _prune(timestamps)

        # Handle a 429 from the last call
        if response is not None and getattr(response, 'status_code', None) == 429:
            ra = response.headers.get("Retry-After", "")
            wait = int(ra) if ra.isdigit() else 20
            time.sleep(wait)
            _prune(timestamps)

        # Recalculate free slots, preserving buffer
        used = len(timestamps)
        free_slots = RATE_LIMIT - used - CALL_BUFFER
        if free_slots <= 0 and timestamps:
            oldest = timestamps[0]
            to_sleep = WINDOW_SECONDS - (time.time() - oldest)
            if to_sleep > 0:
                time.sleep(to_sleep)
            _prune(timestamps)

        # Record this call and immediately persist
        timestamps.append(time.time())
        _save_state(timestamps)


def record_call():
    """
    Record a single API call without any back-off logic.
    """
    with FileLock(LOCK_FILE):
        timestamps = _load_state()
        _prune(timestamps)
        timestamps.append(time.time())
        _save_state(timestamps)


def get_remaining_calls():
    """
    Returns how many more calls you can make right now,
    after accounting for the buffer.
    """
    with FileLock(LOCK_FILE):
        timestamps = _load_state()
        _prune(timestamps)
        used = len(timestamps)
    return max(0, RATE_LIMIT - used - CALL_BUFFER)


def make_api_call(url, **kwargs):
    """
    Wrap your requests.get/post/etc. here to auto-enforce rate limits,
    handle 429s, and retry 5xx errors up to 3 times.
    """
    response = None
    for attempt in range(1, 4):
        enforce_rate_limit(response)
        response = requests.get(url, **kwargs)
        if response.status_code == 429:
            # Let enforce_rate_limit handle the back-off
            continue
        if 500 <= response.status_code < 600:
            time.sleep(5)
            continue
        break
    return response
