# tests/test_individual_api.py
import os
import json
import requests

def test_individual_api_response():
    # Dynamically load the first IRN from your fetched data
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'fca-dashboard', 'data'))
    firm_map_path = os.path.join(base, 'fca_individuals_by_firm.json')
    with open(firm_map_path, 'r', encoding='utf-8') as f:
        firm_map = json.load(f)
    # grab the very first IRN in the map
    first_frn = next(iter(firm_map))
    first_irn = firm_map[first_frn][0]['IRN']
    sample_irn = first_irn

    url = f"https://register.fca.org.uk/services/V0.1/Individuals/{sample_irn}"
    headers = {
        "Accept":       "application/json",
        "X-AUTH-EMAIL": os.getenv("FCA_API_EMAIL"),
        "X-AUTH-KEY":   os.getenv("FCA_API_KEY"),
    }

    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    payload = resp.json()

    data = payload.get("Data")
    assert isinstance(data, list) and data, "Data array is empty or missing"

    record = data[0]
    for field in ("IRN", "Name", "Status", "System Timestamp"):
        assert field in record, f"Expected field '{field}' in individual record"
