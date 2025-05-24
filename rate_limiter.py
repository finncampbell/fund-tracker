# rate_limiter.py
import time
from collections import deque

# Company House rate limit: 600 calls per 5 minutes
RATE_LIMIT = 600
WINDOW_SECONDS = 300.0  # 5 minutes

# Internal timestamp queue
_call_times = deque()

def enforce_rate_limit():
    now = time.time()
    # Drop any timestamps older than WINDOW_SECONDS
    while _call_times and now - _call_times[0] > WINDOW_SECONDS:
        _call_times.popleft()
    # If we've hit the limit, sleep until the oldest timestamp exits the window
    if len(_call_times) >= RATE_LIMIT:
        sleep_for = WINDOW_SECONDS - (now - _call_times[0])
        time.sleep(sleep_for)
        # Prune again after sleeping
        now = time.time()
        while _call_times and now - _call_times[0] > WINDOW_SECONDS:
            _call_times.popleft()

def record_call():
    _call_times.append(time.time())
