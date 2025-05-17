import os

print("GH_FUNDTOKEN present:", bool(os.getenv('GH_FUNDTOKEN')))
print("CH_API_KEY present:", bool(os.getenv('CH_API_KEY')))
print("GH_FUNDTOKEN length:", len(os.getenv('GH_FUNDTOKEN') or '0'))
