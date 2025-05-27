# rate_limiter.py

import time
import json
from collections import deque

# Company House rate limit: 600 calls per 5 minutes
RATE_LIMIT = 600
WINDOW_SECONDS = 300.0  # 5 minutes
BUFFER = 50            # leave 50-call safety buffer

# Internal timestamp queue
_call_times = deque()

def enforce_rate_limit():
    """Block until we're under the allowed call rate (RATE_LIMIT - BUFFER)."""
    now = time.time()
    # prune old timestamps
    while _call_times and now - _call_times[0] > WINDOW_SECONDS:
        _call_times.popleft()

    # if we've hit RATE_LIMIT - BUFFER, sleep until the oldest expires
    while len(_call_times) >= RATE_LIMIT - BUFFER:
        sleep_for = WINDOW_SECONDS - (now - _call_times[0])
        time.sleep(sleep_for)
        now = time.time()
        while _call_times and now - _call_times[0] > WINDOW_SECONDS:
            _call_times.popleft()

def record_call():
    """Record a successful API call timestamp."""
    _call_times.append(time.time())

def get_remaining_calls():
    """Return how many calls remain in this window (after buffer)."""
    now = time.time()
    # prune
    while _call_times and now - _call_times[0] > WINDOW_SECONDS:
        _call_times.popleft()
    return max(0, (RATE_LIMIT - BUFFER) - len(_call_times))

def load_rate_limit_state(filepath):
    """
    Load persisted timestamps from JSON file into our deque,
    pruning anything older than WINDOW_SECONDS.
    """
    try:
        with open(filepath, 'r') as f:
            arr = json.load(f)
    except FileNotFoundError:
        return

    now = time.time()
    for ts in arr:
        if now - ts <= WINDOW_SECONDS:
            _call_times.append(ts)

def save_rate_limit_state(filepath):
    """
    Persist current timestamps (as list) to JSON file.
    """
    # prune before saving
    now = time.time()
    while _call_times and now - _call_times[0] > WINDOW_SECONDS:
        _call_times.popleft()

    with open(filepath, 'w') as f:
        json.dump(list(_call_times), f)
