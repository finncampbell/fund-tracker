import os
import time
import json
from collections import deque

# Company House rate limit: 600 calls per 5 minutes
RATE_LIMIT     = 600
CALL_BUFFER    = 50    # reserve this many calls for live traffic
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
            _call_times.clear()

def _save_state():
    """Persist the current, pruned _call_times to disk."""
    now = time.time()
    while _call_times and now - _call_times[0] > WINDOW_SECONDS:
        _call_times.popleft()
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(list(_call_times), f)
    except Exception:
        pass  # best-effort

# Initialize state on import
_load_state()

def enforce_rate_limit():
    """
    Block if you’ve hit the effective RATE_LIMIT - CALL_BUFFER in WINDOW_SECONDS;
    otherwise return immediately.
    """
    now = time.time()
    # prune old
    while _call_times and now - _call_times[0] > WINDOW_SECONDS:
        _call_times.popleft()
    # effective cap
    cap = RATE_LIMIT - CALL_BUFFER
    if len(_call_times) >= cap:
        sleep_for = WINDOW_SECONDS - (now - _call_times[0])
        time.sleep(sleep_for)
        # prune again
        now = time.time()
        while _call_times and now - _call_times[0] > WINDOW_SECONDS:
            _call_times.popleft()

def record_call():
    """
    Record a new API call timestamp and persist the state. 
    This counts toward the full RATE_LIMIT window, but enforcement only uses the buffered cap.
    """
    ts = time.time()
    _call_times.append(ts)
    _save_state()

def get_remaining_calls() -> int:
    """
    Returns how many calls you can still make before hitting the effective limit
    (RATE_LIMIT - CALL_BUFFER) in the current WINDOW_SECONDS window.
    """
    now = time.time()
    while _call_times and now - _call_times[0] > WINDOW_SECONDS:
        _call_times.popleft()
    cap = RATE_LIMIT - CALL_BUFFER
    return max(0, cap - len(_call_times))
