import time

class RateLimiter:
    """
    Simple sliding-window rate limiter: allows up to max_requests per
    interval (in seconds).  On overflow, sleeps just long enough to
    stay within the limit.
    """
    def __init__(self, max_requests=50, interval=10):
        # e.g. 50 calls per 10 seconds
        self.max_requests = max_requests
        self.interval     = interval
        self.request_times = []  # timestamps of recent requests

    def wait(self):
        """
        Before each new request, purge timestamps older than interval,
        then if we’ve already made max_requests in that window, sleep
        until the window moves forward.
        """
        now = time.time()

        # Keep only those within the last `interval` seconds
        self.request_times = [
            t for t in self.request_times
            if now - t < self.interval
        ]

        # If we’re at capacity, sleep until the oldest timestamp falls out
        if len(self.request_times) >= self.max_requests:
            sleep_for = self.interval - (now - self.request_times[0])
            if sleep_for > 0:
                print(f"Rate limit hit — sleeping {sleep_for:.2f}s")
                time.sleep(sleep_for)

        # Record our new request timestamp
        self.request_times.append(time.time())
