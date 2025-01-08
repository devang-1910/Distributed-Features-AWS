import streamlit as st
import boto3
import pandas as pd
from datetime import datetime
import time
import json

FAILED_RECORDS_HISTORY = []

# Initialize DynamoDB Resource
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')  # Update the region as needed
table_name = 'BookReviews'  # Replace with your DynamoDB table name
table = dynamodb.Table(table_name)


# Function to fetch data from DynamoDB
def fetch_data():
    """Fetch all items from the DynamoDB table."""
    response = table.scan()
    items = response.get('Items', [])

    # Handle pagination for large datasets
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])

    return pd.DataFrame(items)


# Fetch DLQ Records Function
def fetch_failed_records_from_dlq():
    """
    Fetch messages from the DLQ and return as a DataFrame.
    """
    sqs = boto3.client('sqs', region_name='us-east-2')
    dlq_url = "https://sqs.us-east-2.amazonaws.com/490004609496/LambdaDLQ"

    failed_records = []
    try:
        while True:
            # Receive messages from DLQ
            response = sqs.receive_message(
                QueueUrl=dlq_url,
                MaxNumberOfMessages=10,  # Batch fetch for efficiency
                WaitTimeSeconds=2,  # Long-polling for better performance
            )
            messages = response.get("Messages", [])
            if not messages:
                break

            for message in messages:
                body = json.loads(message["Body"])
                failed_records.append(body)
                FAILED_RECORDS_HISTORY.append(body)

    except Exception as e:
        st.error(f"Error fetching DLQ messages: {e}")

    return pd.DataFrame(failed_records)


def format_timestamp(timestamp):
    if isinstance(timestamp, pd.Timestamp):
        return timestamp.strftime('%Y-%m-%d %H:%M:%S')

    if isinstance(timestamp, (str, int)):
        timestamp = int(timestamp)
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

    return timestamp


# Streamlit App Starts
st.title("Distributed System Features Analysis")

# Top Bar: Refresh Button
if st.button("Refresh Data"):
    st.rerun()

# Fetch data from DynamoDB
data = fetch_data()
dlq_data = fetch_failed_records_from_dlq()  # Fetch failed records from DLQ

st.markdown("---")  # Divider for better UI

# Scalability Metrics
st.header("Scalability Metrics")
if not data.empty:
    if 'ingestion_time' in data.columns:
        data['ingestion_time'] = pd.to_datetime(data['ingestion_time'].astype(int), unit='s')
        record_growth = data.groupby(data['ingestion_time'].dt.floor('s')).size()
        st.line_chart(record_growth, height=300)
    else:
        st.write("No 'ingestion_time' column found in data.")
else:
    st.write("No data available for scalability metrics.")

st.markdown("---")  # Divider for better UI

# Real-Time Analysis
st.header("Real-Time Analysis")
if not data.empty:
    if 'sentiment' in data.columns:
        sentiment_counts = data['sentiment'].value_counts()

        # Color-coded bar chart
        sentiment_chart_data = pd.DataFrame({
            "Sentiments": ["Positive", "Negative"],
            "Counts": [
                sentiment_counts.get("Positive", 0),
                sentiment_counts.get("Negative", 0)
            ]
        })

        st.bar_chart(sentiment_chart_data.set_index("Sentiments"), use_container_width=True)
    if 'review_score' in data.columns:
        avg_score = data['review_score'].astype(float).mean()
        st.metric("Average Review Score", f"{avg_score:.2f}")
else:
    st.write("No data available for real-time analysis.")

st.markdown("---")

# Fault Tolerance Metrics
st.header("Fault Tolerance Metrics")
with st.container():
    cols = st.columns(2)

    # Count failed and successful records
    failed_records_in_db = len(data[data['processing_status'] == 'Error']) if not data.empty else 0
    successful_records = len(data[data['processing_status'] == 'Processed']) if not data.empty else 0
    failed_records_in_dlq = len(dlq_data)

    # Combine failed records from DB and DLQ
    total_failed_records = failed_records_in_db + failed_records_in_dlq
    total_records = total_failed_records + successful_records

    # Display metrics
    cols[0].metric("Failed Records", total_failed_records)
    # cols[1].metric("Successful Records", successful_records)
    if total_records > 0:
        failure_rate = (total_failed_records / total_records) * 100
        cols[1].metric("Failure Rate (%)", f"{failure_rate:.2f}")

# Failed Records Details
st.subheader("Failed Records from Dead Letter Queue")
if not dlq_data.empty:
    st.write(dlq_data[['failed_record', 'error_message', 'timestamp']])
else:
    st.write("No failed records found in the DLQ.")

st.markdown("---")

# Raw Data
st.header("Raw Data from DynamoDB Table")
if not data.empty:
    if 'ingestion_time' in data.columns:
        data['ingestion_time'] = data['ingestion_time'].apply(format_timestamp)
    if 'event_time' in data.columns:
        data['event_time'] = data['event_time'].apply(format_timestamp)

    st.write(data)
else:
    st.write("No data available in the DynamoDB table.")

st.markdown("---")


