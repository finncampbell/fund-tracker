# enrich.py

from typing import List, Tuple
from config import CLASSIFICATION_PATTERNS, SIC_LOOKUP

def classify(name: str) -> str:
    """Return first matching category label or 'Other'."""
    for label, pattern in CLASSIFICATION_PATTERNS:
        if pattern.search(name or ""):
            return label
    return "Other"

def enrich_sic(codes: List[str]) -> Tuple[str, str]:
    """
    Given a list of SIC codes, return
      (joined_descriptions, joined_use_cases).
    """
    descs, uses = [], []
    for c in codes or []:
        if c in SIC_LOOKUP:
            d, u = SIC_LOOKUP[c]
            descs.append(d)
            uses.append(u)
    return "; ".join(descs), "; ".join(uses)

def has_target_sic(codes_str: str) -> bool:
    """
    True if the comma-separated SIC string contains any target code.
    """
    if not codes_str or not isinstance(codes_str, str):
        return False
    return any(code.strip() in SIC_LOOKUP for code in codes_str.split(","))
