import logging
import pandas as pd
import re

# --- Filtering patterns and lookups ---

# Name-based ("Fund Entities") patterns for high-precision fund entity detection
CLASS_PATTERNS = [
    (re.compile(r'\bL[\.\-\s]?L[\.\-\s]?P\b', re.IGNORECASE), 'LLP'),
    (re.compile(r'\bL[\.\-\s]?P\b',           re.IGNORECASE), 'LP'),
    (re.compile(r'\bG[\.\-\s]?P\b',           re.IGNORECASE), 'GP'),
    (re.compile(r'\bFund\b',                  re.IGNORECASE), 'Fund'),
    # ... other business-service keywords as desired ...
]

def classify(name):
    """
    Assign a Category to a company name based on CLASS_PATTERNS.
    Returns the label of the first matching pattern, else 'Other'.
    """
    for pat, label in CLASS_PATTERNS:
        if pat.search(name or ''):
            return label
    return 'Other'

# SIC_LOOKUP: dict mapping SIC code string to (description, typical use case)
SIC_LOOKUP = {
    # Example, fill with real data as needed
    "64205": ("Financial services holding companies", "Fund vehicles, Holding companies"),
    "64301": ("Investment trusts", "Fund vehicles"),
    # ... add all relevant codes ...
}

def enrich_sic(codes):
    """
    For a list of SIC codes, join them, and map to descriptions and use cases.
    Returns (joined, description string, use case string).
    """
    joined = ",".join(codes)
    descs, uses = [], []
    for code in codes:
        if code in SIC_LOOKUP:
            d, u = SIC_LOOKUP[code]
            descs.append(d)
            uses.append(u)
    return joined, "; ".join(descs), "; ".join(uses)

def process_companies(raw_companies):
    """
    Process the raw company records, classifying and enriching with SIC info.
    Returns a DataFrame with all master records.
    """
    records = []
    for rec in raw_companies:
        name = rec.get('company_name', '')
        category = classify(name)
        # SIC codes as list of strings
        codes = rec.get('sic_codes', [])
        if not isinstance(codes, list):
            codes = []
        joined, descs, uses = enrich_sic(codes)
        records.append({
            **rec,
            'Category': category,
            'SIC Codes': joined,
            'SIC Description': descs,
            'Typical Use Case': uses,
        })
    return pd.DataFrame(records)

def build_relevant_slice(df_master):
    """
    Build the relevant companies DataFrame: those with fund-entity names or relevant SIC codes.
    """
    # Keyword‐based “Fund Entities” filtering
    mask_cat = df_master['Category'] != 'Other'
    # SIC‐code enrichment and filtering (robust to NaN/float)
    mask_sic = df_master['SIC Codes'].str.split(',').apply(
        lambda codes: any(c in SIC_LOOKUP for c in codes) if isinstance(codes, list) else False
    )
    # The 'relevant' slice: union of fund-entity and SIC matches
    df_rel = df_master[mask_cat | mask_sic]
    return df_rel

def run_for_range(start_date, end_date):
    """
    Fetches, processes, and saves master and relevant company slices for the date range.
    """
    # ... [fetching logic here: get raw_companies for the date range] ...
    # For illustration, let's assume raw_companies is already fetched:
    raw_companies = []  # Replace with actual fetch logic

    # Build master DataFrame with all companies
    df_master = process_companies(raw_companies)
    # Build relevant slice using our two-pronged filtering
    df_rel = build_relevant_slice(df_master)

    # Save or further process as needed
    df_master.to_csv('master_companies.csv', index=False)
    df_rel.to_csv('relevant_companies.csv', index=False)
    logging.info(f"Wrote master ({len(df_master)}) and relevant ({len(df_rel)}) records.")

if __name__ == "__main__":
    # ... [argument parsing and setup code] ...
    # Example: a = parse_args()
    # run_for_range(normalize_date(a.start_date), normalize_date(a.end_date))
    pass  # Replace with actual CLI logic as needed
