#!/usr/bin/env python3
import os
import time
from collections import deque

class RateLimiter:
    def __init__(self, max_calls=50, window_s=10):
        # Allow per-worker override via env-vars RL_MAX_CALLS and RL_WINDOW_S
        self.max_calls = int(os.getenv('RL_MAX_CALLS', max_calls))
        self.window_s  = int(os.getenv('RL_WINDOW_S',  window_s))
        self.calls     = deque()

    def wait(self):
        now = time.time()
        # 1. Drop timestamps older than window_s
        while self.calls and self.calls[0] <= now - self.window_s:
            self.calls.popleft()
        # 2. If we've hit the cap, sleep just enough to free one slot
        if len(self.calls) >= self.max_calls:
            sleep_time = self.window_s - (now - self.calls[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
                now = time.time()
                # purge again after waking
                while self.calls and self.calls[0] <= now - self.window_s:
                    self.calls.popleft()
        # 3. Record this call
        self.calls.append(now)
