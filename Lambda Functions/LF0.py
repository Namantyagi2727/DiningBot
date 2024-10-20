import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

client = boto3.client('lexv2-runtime')

def lambda_handler(event, context):
    logger.debug(f"Received event: {json.dumps(event)}")
    
    try:
        # Check if 'body' exists in the event
        if 'body' in event:
            body = json.loads(event['body'])
        else:
            # If 'body' doesn't exist, assume the event itself is the message
            body = event

        # Extract the user's message
        if 'messages' in body and len(body['messages']) > 0:
            userMessageText = body['messages'][0]['unstructured']['text']
        else:
            raise ValueError("Invalid request format: 'messages' key is missing or empty")

        logger.debug(f"Extracted user message: {userMessageText}")

        # Call Lex
        response = client.recognize_text(
            botId='IPEYS27UMM',
            botAliasId='XCKF4X7HS5',
            localeId='en_US',  # or your chosen locale
            sessionId='testuser',  # You might want to generate a unique session ID
            text=userMessageText
        )
        
        logger.debug(f"Lex response: {json.dumps(response)}")

        # Extract the message from Lex response
        if 'messages' in response:
            for message in response['messages']:
                if message['contentType'] == 'PlainText':
                    bot_response = message['content']
                    break
        else:
            bot_response = "I'm sorry, I couldn't understand that."

        # Format the response
        api_response = {
            'messages': [
                {
                    'type': 'unstructured',
                    'unstructured': {
                        'id': '1',
                        'text': bot_response,
                        'timestamp': 'string'
                    }
                }
            ]
        }

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': json.dumps(api_response)
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': json.dumps({
                'messages': [
                    {
                        'type': 'unstructured',
                        'unstructured': {
                            'id': '1',
                            'text': f"An error occurred: {str(e)}",
                            'timestamp': 'string'
                        }
                    }
                ]
            })
        }