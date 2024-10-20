import time
import random
from yelpapi import YelpAPI
import boto3
from botocore.exceptions import ClientError

# Yelp API credentials
YELP_API_KEY = '59Br9ujWKdJhyUHThUIBZX-xJuY7nLVj7BJW9t1dzyoRJIBKv17cMbWUG1XsHX-jGrv9GcYVqXxSQO2Wob56uTrIEOIExr1cI_Zd5yJIPYvTalmuNwxjaezciMMQZ3Yx'

# AWS DynamoDB settings
DYNAMODB_TABLE_NAME = 'yelp-restaurants'

# Initialize Yelp API client
yelp_api = YelpAPI(YELP_API_KEY)

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

def search_restaurants(cuisine, offset=0):
    try:
        response = yelp_api.search_query(
            term=f'{cuisine} restaurants',
            location='Manhattan',
            limit=50,
            offset=offset
        )
        return response['businesses']
    except Exception as e:
        print(f"Error searching for restaurants: {e}")
        return []

def store_restaurant(restaurant, cuisine):
    try:
        item = {
            'BusinessID': restaurant['id'],
            'Name': restaurant['name'],
            'Address': ', '.join(restaurant['location']['display_address']),
            'Coordinates': {
                'Latitude': str(restaurant['coordinates']['latitude']),
                'Longitude': str(restaurant['coordinates']['longitude'])
            },
            'NumReviews': restaurant['review_count'],
            'Rating': str(restaurant['rating']),
            'ZipCode': restaurant['location']['zip_code'],
            'Cuisine': cuisine,
            'insertedAtTimestamp': int(time.time())
        }
        table.put_item(Item=item)
        print(f"Stored restaurant: {restaurant['name']}")
    except ClientError as e:
        print(f"Error storing restaurant {restaurant['name']}: {e}")

cuisines = ['Chinese', 'Italian', 'Japanese', 'Mexican', 'American']
restaurants_per_cuisine = 1000

for cuisine in cuisines:
    print(f"Scraping {cuisine} restaurants...")
    offset = 0
    cuisine_restaurants = set()
    
    while len(cuisine_restaurants) < restaurants_per_cuisine:
        new_restaurants = search_restaurants(cuisine, offset)
        if not new_restaurants:
            break
        
        for restaurant in new_restaurants:
            if restaurant['id'] not in cuisine_restaurants:
                cuisine_restaurants.add(restaurant['id'])
                store_restaurant(restaurant, cuisine)
        
        offset += 50
        time.sleep(random.uniform(1, 2))  # Respect Yelp API rate limits
    
    print(f"Scraped {len(cuisine_restaurants)} {cuisine} restaurants")

print("Scraping complete!")