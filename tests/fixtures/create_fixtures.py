import json
import requests
import boto3
import os
from decimal import Decimal

def create_event_fixture():
    # Create directory if it doesn't exist
    os.makedirs('tests/fixtures', exist_ok=True)
    
    # Rest of the function remains the same
    response = requests.get("https://511on.ca/api/v2/get/event")
    if response.ok:
        events = json.loads(response.text)[:3]
        
        with open('tests/fixtures/sample_events.json', 'w') as f:
            json.dump(events, f, indent=2)

def create_db_fixture():
    # Create directory if it doesn't exist
    os.makedirs('tests/fixtures', exist_ok=True)
    # Connect to your DynamoDB table
    dynamodb = boto3.resource('dynamodb')
    with open('../../config.json', 'r') as f:
        config = json.load(f)
    table = dynamodb.Table(config['db_name'])
    
    # Get a few items from the table
    response = table.scan(Limit=3)
    
    # Convert Decimal objects to float for JSON serialization
    def decimal_default(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError
    
    # Save to fixture file
    with open('tests/fixtures/sample_db_items.json', 'w') as f:
        json.dump(response['Items'], f, indent=2, default=decimal_default)

if __name__ == "__main__":
    create_event_fixture()
    create_db_fixture()