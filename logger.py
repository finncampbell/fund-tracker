#!/usr/bin/env python3
# logger.py

import os
import logging

# ─── Paths ────────────────────────────────────────────────────────────────────────
LOG_PATH = 'assets/logs'
LOG_FILE = os.path.join(LOG_PATH, 'fund_tracker.log')

# Ensure the log directory exists
os.makedirs(LOG_PATH, exist_ok=True)

# ─── Logger Setup ─────────────────────────────────────────────────────────────────
logger = logging.getLogger('FundTracker')
logger.setLevel(logging.INFO)

# File handler
fh = logging.FileHandler(LOG_FILE)
fh.setLevel(logging.INFO)

# Formatter
formatter = logging.Formatter(
    fmt='%(asctime)s %(name)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
fh.setFormatter(formatter)

# Attach handler
logger.addHandler(fh)

# Expose as 'log'
log = logger
