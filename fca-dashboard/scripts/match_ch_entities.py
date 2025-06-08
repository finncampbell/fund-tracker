import os
import json
from difflib import SequenceMatcher

BASE_DIR        = os.path.dirname(__file__)
CH_PATH         = os.path.join(BASE_DIR, "../data/newcos.json")
FCA_FIRMS_PATH  = os.path.join(BASE_DIR, "../data/fca_firms.json")
FCA_PEOPLE_PATH = os.path.join(BASE_DIR, "../data/fca_individuals.json")
MATCH_OUTPUT    = os.path.join(BASE_DIR, "../data/fca_matches.json")

def load_or_init_json(path, default):
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(default, f, indent=2)
        return default
    with open(path, "r") as f:
        return json.load(f)

def similar(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def main():
    ch_data    = load_or_init_json(CH_PATH, [])
    fca_firms  = load_or_init_json(FCA_FIRMS_PATH, [])
    fca_people = load_or_init_json(FCA_PEOPLE_PATH, [])

    matches = []

    for ent in ch_data:
        ch_name = ent.get("company_name", "")
        directors = ent.get("directors", [])
        base = {
            "company_name": ent.get("company_name"),
            "company_number": ent.get("company_number"),
            "incorporation_date": ent.get("incorporation_date")
        }

        # Name-based matching
        for firm in fca_firms:
            score = similar(ch_name, firm.get("name", ""))
            if score >= 0.9:
                matches.append({
                    **base,
                    "matched_fca_firm": firm["name"],
                    "frn": firm.get("frn"),
                    "match_type": "Name",
                    "match_confidence": "High" if score > 0.95 else "Medium"
                })

        # Director overlap matching
        for person in fca_people:
            pname = person.get("name", "")
            if any(d.lower() in pname.lower() for d in directors):
                matches.append({
                    **base,
                    "matched_fca_person": pname,
                    "linked_firms": person.get("linked_firms", []),
                    "match_type": "Director",
                    "match_confidence": "High"
                })

    with open(MATCH_OUTPUT, "w") as f:
        json.dump(matches, f, indent=2)

    print(f"Saved {len(matches)} matches to {MATCH_OUTPUT}")

if __name__ == "__main__":
    main()
