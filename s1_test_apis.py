import requests
from pprint import pprint

DECLARATION_URL = "https://api.energidataservice.dk/dataset/DeclarationProduction"
ELSPOT_URL = "https://api.energidataservice.dk/dataset/Elspotprices"

def fetch_declaration_sample():
    # small samples: 5 rows, any area
    params = {
        "limit": 5,
        "start": "2022-05-01",
        "end": "2022-05-02",
    }
    resp = requests.get(DECLARATION_URL, params=params)
    resp.raise_for_status()
    data = resp.json()
    # according to their docs, records are in data["records"]
    return data.get("records", [])

def fetch_elspot_sample():
    params = {
        "limit": 5,
        "start": "2022-05-01",
        "end": "2022-05-02",
        }
    resp = requests.get(ELSPOT_URL, params=params)
    resp.raise_for_status()
    data = resp.json()
    return data.get("records", [])

def main():
    print("Fetching DecalationProduction sample...")
    decl_records = fetch_declaration_sample()
    print(f"Got {len(decl_records)} records")
    if decl_records:
        print("First DeclarationProduction record:")
        pprint(decl_records[0])
        
    print("\nFetching Elspotprices sample...")
    elspot_records = fetch_elspot_sample()
    print(f"Got {len(elspot_records)} records")
    if elspot_records:
        print("First Elspotprices record:")
        print(elspot_records[0])
        
if __name__ == "__main__":
        main()