import json
import os
from difflib import SequenceMatcher

CH_PATH = "../data/newcos.json"           # Your Companies House tracker output
FCA_FIRMS_PATH = "../data/fca_firms.json"
FCA_PEOPLE_PATH = "../data/fca_individuals.json"
MATCH_OUTPUT = "../data/fca_matches.json"

def similar(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def main():
    with open(CH_PATH) as f:
        ch_data = json.load(f)
    with open(FCA_FIRMS_PATH) as f:
        fca_firms = json.load(f)
    with open(FCA_PEOPLE_PATH) as f:
        fca_people = json.load(f)

    firm_matches = []
    for entity in ch_data:
        ch_name = entity["company_name"]
        directors = entity.get("directors", [])

        # Match by firm name
        for firm in fca_firms:
            score = similar(ch_name, firm["name"])
            if score >= 0.9:
                firm_matches.append({
                    "company_name": ch_name,
                    "company_number": entity["company_number"],
                    "incorporation_date": entity["incorporation_date"],
                    "matched_fca_firm": firm["name"],
                    "frn": firm["frn"],
                    "match_type": "Name",
                    "match_confidence": "High" if score > 0.95 else "Medium"
                })

        # Match by director overlap
        for person in fca_people:
            for role in person.get("roles", []):
                if role.get("status") != "Removed" and any(d.lower() in person["name"].lower() for d in directors):
                    firm_matches.append({
                        "company_name": ch_name,
                        "company_number": entity["company_number"],
                        "incorporation_date": entity["incorporation_date"],
                        "matched_fca_person": person["name"],
                        "linked_firms": person.get("linked_firms", []),
                        "match_type": "Director",
                        "match_confidence": "High"
                    })

    with open(MATCH_OUTPUT, "w") as f:
        json.dump(firm_matches, f, indent=2)

    print(f"Saved {len(firm_matches)} matches.")

if __name__ == "__main__":
    main()

