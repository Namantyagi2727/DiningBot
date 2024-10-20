import json
import requests
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('yelp-restaurants')  # Ensure this table is created in DynamoDB

# Set up ElasticSearch connection
region = 'us-east-1'  # e.g., 'us-east-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

es = Elasticsearch(
    hosts=[{'host': 'https://search-dining-suggestions-dodmcbms22yu7mll2w2g7qtodq.us-east-1.es.amazonaws.com', 'port': 443}],  # replace with your endpoint
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def lambda_handler(event, context):
    # Retrieve the user preferences from the event
    cuisine = event.get('cuisine', 'American')  # Default to American if not specified
    location = event.get('location', 'New York')  # Default to New York if not specified

    # Construct the Yelp API URL
    url = 'https://api.yelp.com/v3/businesses/search'
    headers = {
        'Authorization': f'Bearer {YELP_API_KEY}'
    }
    params = {
        'term': f'{cuisine} restaurants',
        'location': location,
        'limit': 50  # You can set a limit up to 50
    }

    # Make the API request to Yelp
    response = requests.get(url, headers=headers, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        businesses = data.get('businesses', [])
        
        # Process and store the data in DynamoDB and ElasticSearch
        for business in businesses:
            item = {
                'BusinessID': business['id'],
                'Name': business['name'],
                'Address': business['location']['address1'],
                'Coordinates': str(business['coordinates']),
                'Rating': business['rating'],
                'NumberOfReviews': business['review_count'],
                'ZipCode': business['location'].get('zip_code', '')
            }
            # Store in DynamoDB
            table.put_item(Item=item)

            # Index into ElasticSearch
            es.index(index="restaurants", id=business['id'], body=item)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': f"Retrieved {len(businesses)} restaurants."})
        }
    else:
        return {
            'statusCode': response.status_code,
            'body': json.dumps({'error': response.text})
        }
