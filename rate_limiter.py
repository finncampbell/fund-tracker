import os
import time
import json
import logging
from collections import deque

# Company House rate limit: 600 calls per 5 minutes
RATE_LIMIT     = 600
CALL_BUFFER    = 50     # reserve this many calls
WINDOW_SECONDS = 300.0  # 5 minutes

# Where we persist our sliding‐window state
LOG_DIR    = 'assets/logs'
STATE_FILE = os.path.join(LOG_DIR, 'rate_limit.json')

# In‐memory queue of timestamps
_call_times = deque()

# Set up a logger for debug
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
_rl_log = logging.getLogger("rate_limiter")

def _load_state():
    os.makedirs(LOG_DIR, exist_ok=True)
    if os.path.exists(STATE_FILE):
        try:
            data = json.load(open(STATE_FILE, 'r'))
            now = time.time()
            kept = 0
            for ts in data:
                if now - ts <= WINDOW_SECONDS:
                    _call_times.append(ts)
                    kept += 1
            _rl_log.debug(f"Loaded {len(data)} timestamps; {kept} within {WINDOW_SECONDS}s window")
        except Exception as e:
            _rl_log.warning(f"Error reading {STATE_FILE}, starting fresh: {e}")
            _call_times.clear()
    else:
        _rl_log.debug(f"No existing state file at {STATE_FILE}; starting fresh")

def _save_state():
    now = time.time()
    # prune out‐of‐window
    before = len(_call_times)
    while _call_times and now - _call_times[0] > WINDOW_SECONDS:
        _call_times.popleft()
    after = len(_call_times)
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(list(_call_times), f)
        _rl_log.debug(f"Persisted {after} timestamps ({before-after} pruned) to {STATE_FILE}")
    except Exception as e:
        _rl_log.error(f"Failed to write {STATE_FILE}: {e}")

# Initialize on import
_load_state()
_save_state()

def enforce_rate_limit():
    """
    Block if you’ve hit (RATE_LIMIT − CALL_BUFFER) within WINDOW_SECONDS.
    Prunes stale timestamps on every invocation.
    """
    now = time.time()
    while _call_times and now - _call_times[0] > WINDOW_SECONDS:
        _call_times.popleft()
    cap = RATE_LIMIT - CALL_BUFFER
    if len(_call_times) >= cap:
        oldest = _call_times[0]
        sleep_for = WINDOW_SECONDS - (now - oldest)
        _rl_log.info(f"Rate limit reached ({len(_call_times)}/{cap}); sleeping {sleep_for:.1f}s")
        time.sleep(sleep_for)
        # prune again after waking
        now = time.time()
        while _call_times and now - _call_times[0] > WINDOW_SECONDS:
            _call_times.popleft()
    # now there’s at least one slot

def record_call():
    """Stamp the current time and immediately persist state."""
    ts = time.time()
    _call_times.append(ts)
    _save_state()

def get_remaining_calls() -> int:
    """How many calls remain before (RATE_LIMIT − CALL_BUFFER)."""
    now = time.time()
    while _call_times and now - _call_times[0] > WINDOW_SECONDS:
        _call_times.popleft()
    cap = RATE_LIMIT - CALL_BUFFER
    rem = max(0, cap - len(_call_times))
    _rl_log.debug(f"{len(_call_times)}/{cap} used → {rem} remaining")
    return rem
