import os
import requests
import json
import time
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

EIA_API_KEY = os.getenv('EIA_API_KEY')
KAFKA_BROKER = os.getenv('KAFKA_BROKER')

URL = (
    "https://api.eia.gov/v2/electricity/rto/region-data/data/"
    f"?api_key={EIA_API_KEY}"
    "&frequency=hourly"
    "&data[0]=value"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = 'eia_energy'

def fetch_eia():
    try:
        r = requests.get(URL)
        return r.json()['response']['data']
    except Exception as e:
        print("Error getting URL")
        print(f"ERROR: {e}")
        return None

while True:
    records = fetch_eia()

    if records:
        # Convert string values to floats for numeric fields
        for r in records:
            r['value'] = float(r['value'])

        # Send all records in one message
        producer.send(TOPIC, records)
        print(f"Sent batch of {len(records)} records")

    producer.flush()
    time.sleep(300)