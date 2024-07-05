import csv
import json
import time
from confluent_kafka import Producer
from pydantic import BaseModel, ValidationError
from typing import Optional

# Kafka producer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'demo'
}

# Create Producer instance
producer = Producer(conf)

# Function to handle delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

delay = 0.02  # Delay in seconds between sending each message

# Define the schema using pydantic
class DataSchema(BaseModel):
    index: Optional[int]
    engine: Optional[int]
    cycles: Optional[int]
    alt: Optional[float]
    mach: Optional[float]
    TRA: Optional[float]
    T2: Optional[float]
    T24: Optional[float]
    T30: Optional[float]
    T50: Optional[float]
    P2: Optional[float]
    P15: Optional[float]
    P30: Optional[float]
    Nf: Optional[float]
    Nc: Optional[float]
    epr: Optional[float]
    Ps30: Optional[float]
    phi: Optional[float]
    NRf: Optional[float]
    NRc: Optional[float]
    BPR: Optional[float]
    farB: Optional[float]
    htBleed: Optional[int]
    Nf_dmd: Optional[int]
    PCNfR_dmd: Optional[float]
    W31: Optional[float]
    W32: Optional[float]
    source: Optional[int]

# Define the CSV file path
csv_file_path = '/home/growlt252/Downloads/final_data.csv'

# Read and send each row as a JSON message to Kafka
with open(csv_file_path, 'r') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        try:
            # Validate row data against the schema
            data = DataSchema(**row)
            json_message = data.json()
            producer.produce('demo', value=json_message, callback=delivery_report)
            producer.poll(0)
            time.sleep(delay)  # Introduce a delay between each message
        except ValidationError as e:
            print(f"Validation error: {e}")

producer.flush()
