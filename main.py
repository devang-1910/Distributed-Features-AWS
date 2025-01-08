from random import randint

import pandas as pd
import boto3
import json
import time
from botocore.exceptions import ClientError
from numpy.random import random_integers

# Initialize Kinesis client
kinesis_client = boto3.client('kinesis', region_name='us-east-2')

# Define the Kinesis stream name
stream_name = "LocalDataStream"

def send_to_kinesis(row):
    """
    Send a single record to Kinesis.
    """
    # Convert row to JSON, replacing invalid values
    record = row.to_dict()
    record = {key: (str(value) if value is not None else "N/A") for key, value in record.items()}

    # Handle key names with special characters
    record["review_helpfulness"] = record.pop("review/helpfulness", "0/0")

    # Introduce invalid records programmatically
    if row.name % 50 == 0:  # Inject an invalid record every 50 rows
        invalid_record = record.copy()
        invalid_record["Id"] = None  # Missing required `Id`
        invalid_record["review/score"] = "Invalid"  # Non-numeric value
        record = invalid_record
        print(f"Injected invalid record: {record}")

    # Convert the record to JSON
    record_json = json.dumps(record)

    try:
        # Send the record to Kinesis
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=record_json,
            PartitionKey=str(record.get('Id', 'default'))  # Use `Id` as PartitionKey
        )
        print(f"Data sent to kinesis: {record_json}")
        print(f"Sent record {row.name + 1}: {response['SequenceNumber']}")
    except ClientError as e:
        print(f"Failed to send record {row.name + 1} due to ClientError: {e}")
    except Exception as e:
        print(f"Unexpected error for record {row.name + 1}: {e}")


# Read and stream data row by row
file_path = "Books_rating.csv"

# chunk_size = randint(500, 1000)
# print(chunk_size)

for chunk in pd.read_csv(file_path, chunksize=500):
    # Read one row at a tim
    for _, row in chunk.iterrows():

        send_to_kinesis(row)
        time.sleep(0.1)  # Simulate real-time streaming
