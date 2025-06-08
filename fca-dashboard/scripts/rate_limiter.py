import time

class RateLimiter:
    def __init__(self, max_requests=50, interval=10):
        self.max_requests = max_requests
        self.interval = interval
        self.request_times = []

    def wait(self):
        now = time.time()
        # Keep only recent requests within the interval
        self.request_times = [
            t for t in self.request_times if now - t < self.interval
        ]
        if len(self.request_times) >= self.max_requests:
            sleep_time = self.interval - (now - self.request_times[0])
            if sleep_time > 0:
                print(f"Rate limit hit — sleeping for {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
        self.request_times.append(time.time())

