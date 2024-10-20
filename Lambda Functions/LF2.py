import boto3
import json
import logging
import requests  # Use standalone requests library
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Initialize clients
sqs = boto3.client("sqs")
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns', region_name='us-east-1')  # SNS client for sending email

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/207567754217/DiningConciergeQueue"
ES_ENDPOINT = "https://search-dining-suggestions-dodmcbms22yu7mll2w2g7qtodq.us-east-1.es.amazonaws.com"
DYNAMO_TABLE_NAME = 'restaurants-yelp'
SENDER_EMAIL = "nt2668@nyu.edu"  # Verified email address for SNS

def get_sqs_message():
    """Receive and delete a message from the SQS queue."""
    try:
        response = sqs.receive_message(QueueUrl=QUEUE_URL)
        logger.debug(response)

        message = response.get('Messages', [None])[0]
        if message is None:
            logger.debug("No message in the queue")
            return None

        # Delete the message after processing
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=message['ReceiptHandle'])
        logger.debug('Received and deleted message: %s', message)
        return message
    except ClientError as e:
        logger.error(f"Error fetching SQS message: {str(e)}")
        return None

def lambda_handler(event, context):
    """Main Lambda function entry point."""
    message = get_sqs_message()
    if not message:
        logger.debug("No message to process")
        return {'statusCode': 200, 'body': json.dumps("No message found")}

    try:
        attributes = message["MessageAttributes"]
        cuisine = attributes["cuisine"]["StringValue"]
        location = attributes["location"]["StringValue"]
        time = attributes["time"]["StringValue"]
        num_of_people = attributes["people"]["StringValue"]

    except KeyError as e:
        logger.error(f"Missing key in message attributes: {str(e)}")
        return {'statusCode': 400, 'body': json.dumps("Invalid message format")}

    # Query Elasticsearch
    es_query = f"{ES_ENDPOINT}/_search?q={cuisine}"
    try:
        es_response = requests.get(es_query)
        es_response.raise_for_status()
        data = es_response.json()
        es_data = data.get("hits", {}).get("hits", [])

    except requests.exceptions.RequestException as e:
        logger.error(f"Error querying Elasticsearch: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps("Error querying Elasticsearch")}

    # Extract restaurant IDs
    ids = [restaurant["_source"]["bID"] for restaurant in es_data]

    # Build the message
    message_to_send = (
        f"Hello! Here are my {cuisine} restaurant suggestions in {location} "
        f"for {num_of_people} people, for {time}: "
    )

    # Query DynamoDB for restaurant details
    table = dynamodb.Table(DYNAMO_TABLE_NAME)
    for itr, restaurant_id in enumerate(ids[:5], start=1):  # Limit to 5 results
        try:
            response = table.scan(FilterExpression=Attr('id').eq(restaurant_id))
            item = response.get('Items', [None])[0]
            if not item:
                continue

            name = item["name"]
            address = item["address"]
            message_to_send += f"{itr}. {name}, located at {address}. "

        except ClientError as e:
            logger.error(f"Error querying DynamoDB: {str(e)}")

    message_to_send += "Enjoy your meal!"

    # Send message via SNS email
    try:
        sns_response = sns.publish(
            TopicArn='arn:aws:sns:us-east-1:123456789012:YourSNSTopic',  # Replace with your SNS topic ARN
            Message=message_to_send,
            Subject='Restaurant Suggestions'
        )
        logger.debug("SNS response: %s", json.dumps(sns_response))
        logger.info("Message sent successfully to %s", SENDER_EMAIL)

    except ClientError as e:
        logger.error(f"Error sending message via SNS: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps("Error sending message via SNS")}

    return {'statusCode': 200, 'body': json.dumps("LF2 running successfully")}
