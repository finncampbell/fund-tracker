#!/usr/bin/env python3
import os
import json
import requests
import pandas as pd
from datetime import datetime
from rate_limiter import enforce_rate_limit, record_call
from logger import log  # ← shared logger

# CONFIG
API_BASE       = 'https://api.company-information.service.gov.uk/company'
CH_KEY         = os.getenv('CH_API_KEY')
RELEVANT_CSV   = 'assets/data/relevant_companies.csv'
DIRECTORS_JSON = 'assets/data/directors.json'

def main():
    log.info("Starting director fetch run")
    # … rest unchanged, but use log.info()/log.warning() …
