import time
import json
import threading
from collections import deque

# Company House rate limit: 600 calls per 5 minutes
RATE_LIMIT = 600
WINDOW_SECONDS = 300.0  # 5 minutes
BUFFER = 50            # leave 50-call safety buffer

# Internal timestamp queue and lock
_call_times = deque()
_lock = threading.Lock()

def enforce_rate_limit():
    """
    Block until we're under the allowed call rate (RATE_LIMIT - BUFFER).
    Uses a sliding window of the last WINDOW_SECONDS.
    """
    while True:
        now = time.time()
        with _lock:
            # prune old timestamps
            while _call_times and now - _call_times[0] > WINDOW_SECONDS:
                _call_times.popleft()

            # if we've hit RATE_LIMIT - BUFFER, compute sleep time
            if len(_call_times) < (RATE_LIMIT - BUFFER):
                return  # under limit, proceed immediately

            oldest = _call_times[0]
            wait = (oldest + WINDOW_SECONDS) - now

        # sleep outside lock to allow other threads to wake and prune
        time.sleep(max(wait, 0.01))

def record_call():
    """Record a successful (or attempted) API call timestamp."""
    with _lock:
        _call_times.append(time.time())

def get_remaining_calls() -> int:
    """
    Return how many calls remain in this window (after applying BUFFER).
    """
    now = time.time()
    with _lock:
        # prune old timestamps
        while _call_times and now - _call_times[0] > WINDOW_SECONDS:
            _call_times.popleft()
        remaining = (RATE_LIMIT - BUFFER) - len(_call_times)
    return max(0, remaining)

def load_rate_limit_state(filepath):
    """
    Load persisted timestamps from JSON file into our deque,
    pruning anything older than WINDOW_SECONDS.
    """
    try:
        with open(filepath, 'r') as f:
            arr = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return

    now = time.time()
    with _lock:
        for ts in arr:
            if now - ts <= WINDOW_SECONDS:
                _call_times.append(ts)

def save_rate_limit_state(filepath):
    """
    Persist current timestamps (as list) to JSON file.
    """
    now = time.time()
    with _lock:
        # prune old
        while _call_times and now - _call_times[0] > WINDOW_SECONDS:
            _call_times.popleft()
        arr = list(_call_times)

    with open(filepath, 'w') as f:
        json.dump(arr, f)
