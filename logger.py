# logger.py
import os
import logging

LOG_PATH = 'assets/logs'
LOG_FILE = os.path.join(LOG_PATH, 'fund_tracker.log')

# Ensure log directory exists
os.makedirs(LOG_PATH, exist_ok=True)

# Configure root logger
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Provide a module-level logger
log = logging.getLogger('FundTracker')
