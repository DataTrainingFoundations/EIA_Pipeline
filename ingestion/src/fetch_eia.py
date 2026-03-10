## To read API from .env file
import os

## right now using UTC time zone
from datetime import datetime, timezone

# Used only for type hints (helps readability and IDE suggestions)
# "Any" means the value could be any data type
from typing import Any

# librrary to send HTTP reqs to api
import requests

# It represents the dataset: Hourly Demand, Forecast, Generation, Interchange
BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"

def get_api_key() -> str:
    """
    Read the EIA API key from environment variables.
    """

    api_key = os.getenv("EIA_API_KEY") #################################################################################

    if not api_key:
        raise ValueError("EIA_API_KEY is missing. Add to your env first.")
    return api_key

def build_params(
        start: str = "2025-03-05T00",
        end: str = "2025-03-07T00",
        offset: int = 0,
        length: int = 5000,
) -> dict[str, Any]:
    """
    Build query parameters for the EIA API request.
    """

    # This function constructs the parameters dictionary
    # which will later be passed into the API request
    return{
        # adding api key for authentication
        "api_key": get_api_key(),

        # for now getting hrly data
        "frequency": "hourly",

        # Tells the API which data field we want returned
        # In this case the numeric measurement "value"
        "data[0]": "value",

        # Start time for query window
        "start": start,

        # End time for query window
        "end": end,

        # sort results by column "period"
        "sort[0][column]": "period",

        # sort diection descending (latest first)
        "sort[0][direction]": "desc",

        # Offset controls pagination start
        # If we had multiple pages we would increase this
        "offset": offset,

        # Maximum rows to return in one API request
        "length": length,
    }

def fetch_eia_raw(
        start: str = "2025-03-05T00",
    end: str = "2025-03-07T00",
    offset: int = 0,
    length: int = 5000,
) -> dict[str, Any]:
    """
    Fetch raw JSON response from the EIA API.
    """

    # Build query parameters using our helper function
    params = build_params(start=start, end=end, offset=offset, length=length)

    # Send HTTP GET request to EIA API
    # BASE_URL = dataset endpoint
    # params = query parameters
    # timeout prevents hanging requests
    response = requests.get(BASE_URL, params=params, timeout=30)

    # If API returned an error (400, 500 etc) this will raise exception
    response.raise_for_status()

    # Convert HTTP response JSON into Python dictionary
    payload = response.json()

    # Verify response format contains expected "response" object
    if "response" not in payload:
        raise ValueError(f"Unexpected API response format: {payload}")
    
    # Return the full JSON payload
    return payload

def safe_float(value: Any) -> float | None:
    """
    Convert value to float safely.
    """

    try:
        # Many APIs return numbers as strings. Example: "12345". This converts them into numeric float values
        return float(value) if value is not None else None 
    # If conversion fails (bad data, empty string, etc)
    except (TypeError, ValueError):
        # instead of letting pipeline crash return none
        return None
    
def normalize_record(row: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize one raw EIA row into a cleaner project-friendly structure.

    We keep original fields too where useful, because later Spark/Silver can decide
    what to keep or rename.
    """

    # Convert raw API row into a standardized structure
    return {

        # Timestamp for the measurement
        "period": row.get("period"),

        # Balancing authority / region
        "respondent": row.get("respondent"),

        # Human readable respondent name
        "respondent_name": row.get("respondent-name"),

        # Metric type code (ex: demand, forecast, generation etc)
        "type": row.get("type"),

        # Human readable metric type
        "type_name": row.get("type-name"),

        # Actual measurement value converted safely to float
        "value": safe_float(row.get("value")),

        # Units for measurement (usually MW)
        "value_units": row.get("value-units"),

        # Timezone information
        "timezone": row.get("timezone"),

        # Metadata showing where this record originated
        "source": "EIA",

        # Dataset identifier (useful later in lakehouse architecture)
        "dataset": "electricity/rto/region-data",

        # Timestamp showing when this pipeline ingested the record
        "ingested_at": datetime.now(timezone.utc).isoformat(),

        # Store full raw record for traceability and debugging
        "raw_record": row,
    }


def extract_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Extract and normalize records from the EIA API payload.
    """

    # Navigate JSON structure:
    # payload["response"]["data"]
    rows = payload.get("response", {}).get("data", [])

    # Normalize every row using list comprehension
    return [normalize_record(row) for row in rows]

def fetch_eia_records(
    start: str = "2025-03-05T00",
    end: str = "2025-03-07T00",
    offset: int = 0,
    length: int = 5000,
) -> list[dict[str, Any]]:
    """
    Main helper for the rest of the pipeline:
    fetch raw payload -> extract normalized records.
    """

    # Step 1: fetch raw JSON response from API
    payload = fetch_eia_raw(start=start, end=end, offset=offset, length=length)

    # Step 2: extract clean normalized records
    return extract_records(payload)    

# This block runs only if the file is executed directly
# Example: python fetch_eia.py
if __name__ == "__main__":

    # Fetch records from the API
    records = fetch_eia_records()

    # Print how many records were retrieved
    print(f"Fetched {len(records)} records from EIA")

    # Print the first 5 records for quick inspection
    for record in records[:5]:
        print(record)



## Structure of fetch

## EIA API
##   ↓
## fetch_eia.py
##   ↓
## clean normalized records ------------------------------------
##   ↓
## NOT THESE
##   ↓
## publish_kafka.py
##   ↓
## Kafka Topic
##   ↓
## Spark Bronze Layer
