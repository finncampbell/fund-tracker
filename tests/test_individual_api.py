# tests/test_individual_api.py
"""
Smoke test for the FCA Individual API endpoint.
Fetches a sample IRN and verifies the response structure.
"""
import os
import requests

def test_individual_api_response():
    # Replace with a known-good IRN from your data set
    sample_irn = "1000103"
    url = f"https://register.fca.org.uk/services/V0.1/Individuals/{sample_irn}"
    headers = {
        "Accept":       "application/json",
        "X-AUTH-EMAIL": os.getenv("FCA_API_EMAIL"),
        "X-AUTH-KEY":   os.getenv("FCA_API_KEY"),
    }

    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    payload = resp.json()

    # Ensure Data array exists and has at least one record
    assert "Data" in payload, "Response missing 'Data' key"
    data = payload["Data"]
    assert isinstance(data, list) and data, "Data array is empty"

    record = data[0]
    # Check core fields are present
    for field in ("IRN", "Name", "Status", "System Timestamp"):
        assert field in record, f"Expected field '{field}' in individual record"
