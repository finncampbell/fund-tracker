#!/usr/bin/env python3
import os
import json
import logging
import requests
import pandas as pd
from datetime import datetime
from rate_limiter import enforce_rate_limit, record_call

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
API_BASE        = 'https://api.company-information.service.gov.uk/company'
CH_KEY          = os.getenv('CH_API_KEY')
RELEVANT_CSV    = 'assets/data/relevant_companies.csv'
DIRECTORS_JSON  = 'assets/data/directors.json'
LOG_FILE        = 'director_fetch.log'

# ─── LOGGING SETUP ───────────────────────────────────────────────────────────────
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)

def load_relevant_numbers():
    df = pd.read_csv(RELEVANT_CSV, dtype=str)
    return set(df['C]()
