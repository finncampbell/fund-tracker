import os
import time
import json
from collections import deque

# Company House rate limit: 600 calls per 5 minutes
RATE_LIMIT     = 600
CALL_BUFFER    = 50     # reserve this many calls for live traffic
WINDOW_SECONDS = 300.0  # 5 minutes

# Path to persist timestamps
LOG_DIR    = 'assets/logs'
STATE_FILE = os.path.join(LOG_DIR, 'rate_limit.json')

# In-memory queue of call timestamps
_call_times = deque()

def _load_state():
    """Load persisted timestamps into _call_times, pruning older than WINDOW_SECONDS."""
    os.makedirs(LOG_DIR, exist_ok=True)
    if os.path.exists(STATE_FILE):
        try:
            data = json.load(open(STATE_FILE, 'r'))
            now = time.time()
            for ts in data:
                if now - ts <= WINDOW_SECONDS:
                    _call_times.append(ts)
        except Exception:
            # corrupted or unreadable file: start fresh
            _call_times.clear()

def _save_state():
    """Persist the current, pruned _call_times to disk (best-effort)."""
    now = time.time()
    # prune out timestamps older than WINDOW_SECONDS
    while _call_times and now - _call_times[0] > WINDOW_SECONDS:
        _call_times.popleft()
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(list(_call_times), f)
    except Exception:
        pass  # ignore write errors

# Initialize state on import and ensure the state file exists (even if empty)
_load_state()
_save_state()

def enforce_rate_limit():
    """
    Block if you’ve hit the effective limit (RATE_LIMIT - CALL_BUFFER) within WINDOW_SECONDS;
    otherwise return immediately.
    """
    now = time.time()
    # prune out-of-window timestamps
    while _call_times and now - _call_times[0] > WINDOW_SECONDS:
        _call_times.popleft()
    cap = RATE_LIMIT - CALL_BUFFER
    if len(_call_times) >= cap:
        # compute sleep time until oldest timestamp exits the window
        sleep_for = WINDOW_SECONDS - (now - _call_times[0])
        time.sleep(sleep_for)
        # prune again after sleeping
        now = time.time()
        while _call_times and now - _call_times[0] > WINDOW_SECONDS:
            _call_times.popleft()

def record_call():
    """
    Record a new API call timestamp and persist the state.
    This counts toward the full 600-window but enforcement uses the buffered cap.
    """
    ts = time.time()
    _call_times.append(ts)
    _save_state()

def get_remaining_calls() -> int:
    """
    Return how many calls you can still make before hitting the effective cap
    (RATE_LIMIT - CALL_BUFFER) in the current WINDOW_SECONDS window.
    """
    now = time.time()
    # prune out-of-window timestamps
    while _call_times and now - _call_times[0] > WINDOW_SECONDS:
        _call_times.popleft()
    cap = RATE_LIMIT - CALL_BUFFER
    return max(0, cap - len(_call_times))
