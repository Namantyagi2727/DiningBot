import json
import boto3
import csv


def lambda_handler(event, context):
    # TODO implement
    region = 'us-east-1'
    record_list = []
    count = 0

    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        s3_resource = boto3.resource('s3')
        dynamodb = boto3.client('dynamodb', region_name=region)

        print('Bucket : ', bucket, ',  Key : ', key)

        s3_object = s3_resource.Object(bucket, key)
        data = s3_object.get()['Body'].read().decode('utf-8').splitlines()
        rows = csv.reader(data)
        #headers = next(data)
        #table = dynamodb.Table('yelp-restaurants')

        for row in rows:

            if len(row) == 10:
                restaurantId = row[0]
                restaurantName = row[1]
                cuisineType = row[2]
                restaurantAddress = row[3]
                longitude = row[4]
                latitude = row[5]
                restaurantRating = row[6]
                reviewCount = row[7]
                priceRange = row[8]
                timestamp = row[9]
                count += 1
            if priceRange == None:
                priceRange = '$'

            add_to_db = dynamodb.put_item(
                TableName='yelp-restaurants',
                Item={
                    'restaurantId': {'S': str(restaurantId)},
                    'restaurantName': {'S': str(restaurantName)},
                    'cuisineType': {'S': str(cuisineType)},
                    'restaurantAddress': {'S': str(restaurantAddress)},
                    'longitude': {'N': str(longitude)},
                    'latitude': {'N': str(latitude)},
                    'restaurantRating': {'N': str(restaurantRating)},
                    'reviewCount': {'N': str(reviewCount)},
                    'priceRange': {'S': str(priceRange)},
                    'insertedAtTimestamp': {'S': str(timestamp)},
                })

        print('Successfully read {} records'.format(count))
        print('Successfully inserted items into table : "yelp-restaurants"!')

    except Exception as e:
        print(str(e))

    return {
        'statusCode': 200,
        'body': json.dumps('CSV to DynamoDB Lambda Function.')
    }