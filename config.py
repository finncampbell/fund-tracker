import re

# ─── Column schema ───────────────────────────────────────────────────────────────
# Exact order for master_companies.csv/.xlsx
COLUMN_SCHEMA = [
    "CompanyNumber",
    "CompanyName",
    "IncorporationDate",
    "Status",
    "Source",
    "DateDownloaded",
    "TimeDiscovered",
    "SIC Codes",
    "Category",
    "SIC Description",
    "Typical Use Case",
]

# ─── Classification regexes ─────────────────────────────────────────────────────
# In priority order: first match wins
CLASSIFICATION_PATTERNS = [
    ("LLP",         re.compile(r"\bL[\.\-\s]?L[\.\-\s]?P\b", re.I)),
    ("LP",          re.compile(r"\bL[\.\-\s]?P\b",         re.I)),
    ("GP",          re.compile(r"\bG[\.\-\s]?P\b",         re.I)),
    ("Fund",        re.compile(r"\bFund\b",                re.I)),
    ("Ventures",    re.compile(r"\bVentures?\b",           re.I)),
    ("Investments", re.compile(r"\bInvestments?\b",        re.I)),
    ("Capital",     re.compile(r"\bCapital\b",             re.I)),
    ("Equity",      re.compile(r"\bEquity\b",              re.I)),
    ("Advisors",    re.compile(r"\bAdvisors?\b|\bAdvisers?\b", re.I)),
    ("Partners",    re.compile(r"\bPartners?\b",           re.I)),
]

# ─── SIC lookup table (16 target codes) ─────────────────────────────────────────
# Each maps to (Description, Typical Use Case)
SIC_LOOKUP = {
    '64205': ("Activities of financial services holding companies",
              "Holding-company SPVs, co-investment vehicles, master/feeder hubs."),
    '64209': ("Activities of other holding companies n.e.c.",
              "Protected-cell SPVs, bespoke feeders."),
    '64301': ("Activities of investment trusts",
              "Closed-ended listed trusts."),
    '64302': ("Activities of unit trusts",
              "On-shore feeder trusts."),
    '64303': ("Activities of venture and development capital companies",
              "Venture Capital Trusts (VCTs)."),
    '64304': ("Activities of open-ended investment companies",
              "OEIC umbrella layers."),
    '64305': ("Activities of property unit trusts",
              "Property-unit-trust vehicles."),
    '64306': ("Activities of real estate investment trusts",
              "UK-regulated REITs."),
    '64921': ("Credit granting by non-deposit-taking finance houses",
              "Direct-lending SPVs."),
    '64922': ("Activities of mortgage finance companies",
              "Mortgage-backed SPVs."),
    '64929': ("Other credit granting n.e.c.",
              "Mezzanine/debt hybrid vehicles."),
    '64991': ("Security dealing on own account",
              "CLO collateral-management SPVs."),
    '64999': ("Financial intermediation not elsewhere classified",
              "Catch-all credit-oriented SPVs."),
    '66300': ("Fund management activities",
              "AIFMs and portfolio-management firms."),
    '70100': ("Activities of head offices",
              "Group HQ: strategy/finance/compliance."),
    '70221': ("Financial management (of companies and enterprises)",
              "Treasury and internal finance arm."),
}
