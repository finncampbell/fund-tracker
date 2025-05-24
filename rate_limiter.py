import time
from collections import deque

# Company House rate limit
RATE_LIMIT = 600        # max calls
WINDOW_SECONDS = 300.0  # per 5 minutes

# internal timestamp queue
_call_times = deque()

def enforce_rate_limit():
    now = time.time()
    # drop any calls older than WINDOW_SECONDS
    while _call_times and now - _call_times[0] > WINDOW_SECONDS:
        _call_times.popleft()
    if len(_call_times) >= RATE_LIMIT:
        # sleep until oldest timestamp is just outside the window
        sleep_for = WINDOW_SECONDS - (now - _call_times[0])
        time.sleep(sleep_for)
        # prune again
        now = time.time()
        while _call_times and now - _call_times[0] > WINDOW_SECONDS:
            _call_times.popleft()

def record_call():
    _call_times.append(time.time())
