from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3

# Define AWS credentials and region
region = 'your-region'  # e.g., 'us-east-1'
service = 'es'

credentials = boto3.Session().get_credentials()
aws_auth = AWS4Auth(credentials.access_key, credentials.secret_key, credentials.token, region, service)

# Define OpenSearch domain endpoint
host = 'https://your-domain-endpoint'  # e.g., 'https://search-restaurants-domain-abc123.us-east-1.es.amazonaws.com'

# Create a connection to OpenSearch
es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=aws_auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

### 1. Create an index called "restaurants" ###
def create_index():
    index_body = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        }
    }

    response = es.indices.create(index='restaurants', body=index_body, ignore=400)  # 400 ignores index already exists
    print(f'Create index response: {response}')

### 2. Define a mapping for the "Restaurant" type ###
def create_mapping():
    mapping_body = {
        "properties": {
            "RestaurantID": {
                "type": "text"
            },
            "Cuisine": {
                "type": "text"
            }
        }
    }

    response = es.indices.put_mapping(index='restaurants', body=mapping_body, doc_type='Restaurant')
    print(f'Create mapping response: {response}')

### 3. Insert restaurant data ###
def insert_restaurant(restaurant_id, cuisine):
    document = {
        "RestaurantID": restaurant_id,
        "Cuisine": cuisine
    }

    response = es.index(index='restaurants', doc_type='Restaurant', body=document)
    print(f'Insert document response: {response}')


# --- Execute the Operations ---

# 1. Create the index
create_index()

# 2. Define the mapping for the "Restaurant" type
create_mapping()

# 3. Insert sample restaurant data
insert_restaurant("12345", "Chinese")
insert_restaurant("67890", "Italian")
