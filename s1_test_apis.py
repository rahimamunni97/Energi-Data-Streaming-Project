import requests
from pprint import pprint

# Base URLs for the two Energinet datasets used in the project.
# These endpoints return JSON responses with time-series energy data.
DECLARATION_URL = "https://api.energidataservice.dk/dataset/DeclarationProduction"
ELSPOT_URL = "https://api.energidataservice.dk/dataset/Elspotprices"


def fetch_declaration_sample():
    """
    Fetch a small sample from the DeclarationProduction dataset.

    This function is mainly used during development to understand
    the structure of the production data returned by the API
    before integrating it into the streaming pipeline.
    """
    # Request only a small number of rows to avoid large responses
    # while inspecting the data format and available fields
    params = {
        "limit": 5,
        "start": "2022-05-01",
        "end": "2022-05-02",
    }

    resp = requests.get(DECLARATION_URL, params=params)

    # Raise an exception if the API request fails (e.g. network or HTTP errors)
    resp.raise_for_status()

    data = resp.json()

    # According to the Energinet API documentation, actual records
    # are stored inside the "records" field of the JSON response
    return data.get("records", [])


def fetch_elspot_sample():
    """
    Fetch a small sample from the ElspotPrices dataset.

    This function mirrors the declaration sample fetch and is
    used to verify timestamp alignment and price fields.
    """
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
    """
    Simple test entry point used to manually inspect API responses.
    This script is not part of the streaming pipeline itself,
    but was useful during early experimentation and debugging.
    """

    print("Fetching DeclarationProduction sample...")
    decl_records = fetch_declaration_sample()
    print(f"Got {len(decl_records)} records")

    # Print only the first record to inspect field names and values
    if decl_records:
        print("First DeclarationProduction record:")
        pprint(decl_records[0])

    print("\nFetching Elspotprices sample...")
    elspot_records = fetch_elspot_sample()
    print(f"Got {len(elspot_records)} records")

    if elspot_records:
        print("First Elspotprices record:")
        pprint(elspot_records[0])


if __name__ == "__main__":
    # Run the script directly for quick testing
    main()
