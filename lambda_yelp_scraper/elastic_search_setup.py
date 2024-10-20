import boto3
import requests
import json
from requests_aws4auth import AWS4Auth

# AWS region and service
region = 'us-east-1'  # Change to your region
service = 'es'

# AWS credentials
credentials = boto3.Session().get_credentials()
aws_auth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# Elasticsearch (OpenSearch) endpoint
es_host = 'https://search-dining-suggestions-dodmcbms22yu7mll2w2g7qtodq.us-east-1.es.amazonaws.com'  # Replace with your OpenSearch (Elasticsearch) endpoint

# Elasticsearch index name
index_name = 'restaurants'

# Index mapping
index_mapping = {
    "mappings": {
        "properties": {
            "BusinessID": {"type": "keyword"},
            "Name": {"type": "text"},
            "Cuisine": {"type": "text"},
            "Rating": {"type": "float"},
            "Address": {"type": "text"},
            "Coordinates": {"type": "geo_point"},
            "NumberOfReviews": {"type": "integer"},
            "ZipCode": {"type": "text"}
        }
    }
}

# Function to create Elasticsearch index
def create_index():
    url = f'{es_host}/{index_name}'
    headers = {"Content-Type": "application/json"}
    
    # Check if the index exists
    response = requests.head(url, auth=aws_auth)
    if response.status_code == 404:
        print(f"Creating index {index_name}...")
        response = requests.put(url, auth=aws_auth, headers=headers, data=json.dumps(index_mapping))
        if response.status_code == 200:
            print(f"Index {index_name} created successfully!")
        else:
            print(f"Failed to create index: {response.text}")
    else:
        print(f"Index {index_name} already exists!")

# Function to insert a restaurant document into Elasticsearch
def insert_restaurant(restaurant_data):
    url = f'{es_host}/{index_name}/_doc'
    headers = {"Content-Type": "application/json"}

    response = requests.post(url, auth=aws_auth, headers=headers, data=json.dumps(restaurant_data))
    
    if response.status_code == 201:
        print(f"Inserted document: {restaurant_data['Name']}")
    else:
        print(f"Failed to insert document: {response.text}")

# Example list of restaurants to index
restaurants = [
    {
        "BusinessID": "1",
        "Name": "Italian Bistro",
        "Cuisine": "Italian",
        "Rating": 4.5,
        "Address": "123 Main Street, New York, NY",
        "Coordinates": {"lat": 40.7128, "lon": -74.0060},
        "NumberOfReviews": 124,
        "ZipCode": "10001"
    },
    {
        "BusinessID": "2",
        "Name": "Sushi Palace",
        "Cuisine": "Japanese",
        "Rating": 4.2,
        "Address": "456 Park Avenue, New York, NY",
        "Coordinates": {"lat": 40.7580, "lon": -73.9855},
        "NumberOfReviews": 98,
        "ZipCode": "10022"
    }
    # Add more restaurant data here
]

# Main function to set up the index and insert data
def main():
    create_index()  # Create the index in Elasticsearch
    
    # Insert each restaurant record
    for restaurant in restaurants:
        insert_restaurant(restaurant)

if __name__ == '__main__':
    main()
