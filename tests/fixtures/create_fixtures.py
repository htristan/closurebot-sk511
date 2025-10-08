"""
Script to create test fixtures from real SK511 API data and DynamoDB.
This ensures our tests use realistic Saskatchewan highway data.
"""
import json
import requests
import boto3
import os
from decimal import Decimal

def create_event_fixture():
    # Create directory if it doesn't exist
    os.makedirs('tests/fixtures', exist_ok=True)
    
    # Fetch real SK511 API data for testing
    response = requests.get("https://hotline.gov.sk.ca/api/v2/get/event")
    if response.ok:
        events = json.loads(response.text)[:3]
        
        with open('tests/fixtures/sample_events.json', 'w') as f:
            json.dump(events, f, indent=2)
        print("✅ Event fixture created successfully from SK511 API")
    else:
        print(f"❌ Failed to fetch SK511 API data: {response.status_code}")

def create_db_fixture():
    # Create directory if it doesn't exist
    os.makedirs('tests/fixtures', exist_ok=True)
    
    try:
        # Connect to your DynamoDB table
        dynamodb = boto3.resource('dynamodb')
        with open('config.json', 'r') as f:
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
        print("✅ Database fixture created successfully")
    except Exception as e:
        print(f"⚠️  Could not create database fixture (AWS credentials or table not available): {e}")
        print("   This is normal if you don't have AWS credentials configured.")

if __name__ == "__main__":
    create_event_fixture()
    create_db_fixture()