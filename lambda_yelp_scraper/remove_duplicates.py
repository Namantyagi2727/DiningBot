import boto3
from collections import defaultdict

# Initialize the DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('yelp-restaurants')  # Use the correct table name

def scan_table():
    """
    Scans the DynamoDB table and returns all items.
    """
    response = table.scan()
    data = response['Items']
    
    # Handle pagination if more data exists
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    
    return data

def remove_duplicates():
    """
    Finds and removes duplicate entries from the table based on BusinessID.
    """
    # Dictionary to store seen BusinessIDs and their entries
    seen_business_ids = defaultdict(list)
    
    # Scan all the items in the table
    all_items = scan_table()
    
    # Iterate through all items
    for item in all_items:
        business_id = item['BusinessID']
        
        # Add the item to the list of seen BusinessIDs
        seen_business_ids[business_id].append(item)
    
    # Remove duplicates by keeping only the first occurrence
    for business_id, items in seen_business_ids.items():
        if len(items) > 1:
            print(f"Found {len(items)} duplicates for BusinessID: {business_id}. Removing duplicates...")
            
            # Keep the first item and remove the rest
            for item in items[1:]:
                delete_item(item['BusinessID'], item['Name'])
            print(f"Duplicates for {business_id} have been removed.")
        else:
            print(f"No duplicates found for BusinessID: {business_id}")

def delete_item(business_id, name):
    """
    Deletes an item from the DynamoDB table based on the BusinessID and Name.
    """
    table.delete_item(
        Key={
            'BusinessID': business_id,
            'Name': name  # Make sure to specify the correct key attributes
        }
    )
    print(f"Deleted duplicate entry: BusinessID: {business_id}, Name: {name}")

if __name__ == '__main__':
    remove_duplicates()
