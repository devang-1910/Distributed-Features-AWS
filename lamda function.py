import json
import boto3
import base64
import logging
import time

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB client
dynamodb_client = boto3.client('dynamodb')
# Initialize SQS Client --------------------
sqs_client = boto3.client('sqs')

# Replace with your DLQ URL----------------
DLQ_URL = "https://sqs.us-east-2.amazonaws.com/490004609496/LambdaDLQ"


# Message Management to DLQ-----------------
def send_to_dlq(payload, error_message):
    """
    Send a failed record to the Dead Letter Queue (DLQ).
    """
    try:
        sqs_client.send_message(
            QueueUrl=DLQ_URL,
            MessageBody=json.dumps({
                "failed_record": payload,
                "error_message": error_message,
                "timestamp": time.time()
            })
        )
        logger.info("Successfully sent failed record to DLQ.")
    except Exception as e:
        logger.error(f"Failed to send record to DLQ: {e}")


def lambda_handler(event, context):
    """
    Lambda function to process records from Kinesis Data Stream and store them in DynamoDB.
    """
    try:
        for record in event['Records']:
            try:
                # Decode the base64-encoded Kinesis data
                raw_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
                logger.info(f"Raw decoded data from Kinesis: {raw_data}")

                # Parse JSON payload
                payload = json.loads(raw_data)
                logger.info(f"Decoded payload: {payload}")

                # Validate `Id` field (partition key for DynamoDB)------------------
                if not payload.get('Id'):
                    raise ValueError("Missing required field: 'Id'")

                # Validate numeric fields------------------------------
                try:
                    review_score = float(payload.get('review_score', 0))
                except ValueError:
                    raise ValueError(f"Invalid 'review_score': {payload.get('review_score')}")

                # Perform sentiment analysis or derived fields
                sentiment = "Positive" if float(payload.get('review/score', 0)) > 3 else "Negative"

                # Get current ingestion time (Unix timestamp)
                ingestion_time = int(time.time())

                # Get event time if available, else default to ingestion_time
                event_time = payload.get('event_time', ingestion_time)

                # Construct DynamoDB item
                item = {
                    'Id': {'S': str(payload.get('Id', 'N/A'))},
                    'User_id': {'S': str(payload.get('User_id', 'N/A'))},
                    'review_score': {'N': str(payload.get('review/score', 0))},
                    'sentiment': {'S': sentiment},
                    'review_summary': {'S': payload.get('review/summary', 'N/A')},
                    'review_text': {'S': payload.get('review/text', 'N/A')},
                    'ingestion_time': {'N': str(ingestion_time)},
                    'event_time': {'N': str(event_time)},
                    'processing_status': {'S': 'Processed'}  # Default to "Processed" initially
                }

                logger.info(f"Constructed DynamoDB item: {item}")

                # Insert item into DynamoDB
                dynamodb_client.put_item(
                    TableName='BookReviews',  # Replace with your DynamoDB table name
                    Item=item
                )
                logger.info(f"Successfully inserted item with Id: {payload.get('Id')}")

            except ValueError as e:
                # Handle validation errors and send record to DLQ
                logger.error(f"Validation Error: {e}")
                send_to_dlq(payload, f"Validation Error: {e}")

            except Exception as e:
                # Handle unexpected errors and send record to DLQ
                logger.error(f"Error processing record: {record}. Error: {e}")
                send_to_dlq(record, f"ProcessingError: {e}")

        return {"statusCode": 200, "body": "Success"}

    except Exception as e:
        logger.error(f"Error processing records: {e}")
        return {"statusCode": 500, "body": "Failure"}
