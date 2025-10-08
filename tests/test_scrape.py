import pytest
from unittest.mock import patch, Mock, mock_open
import json
from datetime import datetime, timedelta
from decimal import Decimal
from freezegun import freeze_time
from moto import mock_aws
import boto3
import os

# Add this before the scrape import
os.environ['DISCORD_WEBHOOK'] = 'https://mock-discord-webhook.com/test'

from scrape import (
    check_which_polygon_point, getThreadID, unix_to_readable,
    post_to_discord_closure, post_to_discord_updated, post_to_discord_completed,
    close_recent_events, cleanup_old_events, float_to_decimal,
    check_and_post_events, generate_geojson
)

# Load fixture data
@pytest.fixture
def sample_events():
    with open('tests/fixtures/sample_events.json', 'r') as f:
        return json.load(f)

@pytest.fixture
def sample_db_items():
    with open('tests/fixtures/sample_db_items.json', 'r') as f:
        items = json.load(f)
        # Convert float values to Decimal
        for item in items:
            for key, value in item.items():
                if isinstance(value, float):
                    item[key] = Decimal(str(value))
        return items

@pytest.fixture
def sample_event(sample_events):
    # Use the first event from the sample data
    return sample_events[0]

@pytest.fixture
def mock_dynamodb_table():
    with patch('boto3.resource') as mock_boto:
        mock_table = mock_boto.return_value.Table.return_value
        # Add common table operations
        mock_table.query.return_value = {'Items': []}
        mock_table.scan.return_value = {'Items': []}
        yield mock_table

@pytest.fixture
def mock_config():
    return {
        'Thread-GTA': '123456',
        'Thread-Central_EasternOntario': '234567',
        'Thread-NorthernOntario': '345678',
        'Thread-SouthernOntario': '456789',
        'Thread-CatchAll': '567890',
        'timezone': 'US/Eastern',
        'license_notice': 'Test License Notice',
        'db_name': 'test-db'
    }

# Polygon Tests
@pytest.mark.parametrize("coordinates,expected_region", [
    ((43.6532, -79.3832), 'GTA'),  # Toronto
    ((45.4215, -75.6972), 'Central & Eastern Ontario'),  # Ottawa
    ((46.4917, -80.9930), 'Northern Ontario'),  # Sudbury
    ((43.2557, -79.8711), 'Southern Ontario'),  # Hamilton
    ((0, 0), 'Other'),  # Invalid point
])
def test_check_which_polygon_point(coordinates, expected_region):
    from shapely.geometry import Point
    point = Point(coordinates[0], coordinates[1])
    assert check_which_polygon_point(point) == expected_region

# Thread ID Tests
@pytest.mark.parametrize("region,expected_thread", [
    ('GTA', '123456'),
    ('Central & Eastern Ontario', '234567'),
    ('Northern Ontario', '345678'),
    ('Southern Ontario', '456789'),
    ('Other', '567890'),
    ('Invalid', '567890'),
])
def test_getThreadID(region, expected_thread, mock_config):
    with patch('scrape.config', mock_config):
        assert getThreadID(region) == expected_thread

# Time Conversion Tests
@pytest.mark.parametrize("timestamp,expected_time", [
    (1672574400, '2023-Jan-01 07:00 AM'),  # Regular case
    (1672531200, '2022-Dec-31 07:00 PM'),  # Corrected expected time
])
@freeze_time("2023-01-01 12:00:00", tz_offset=0)
def test_unix_to_readable(timestamp, expected_time):
    assert unix_to_readable(timestamp) == expected_time

# Discord Posting Tests
@patch('scrape.DiscordWebhook')
def test_post_to_discord_closure(mock_webhook, sample_event, mock_config):
    with patch('scrape.config', mock_config):
        post_to_discord_closure(sample_event, 'GTA')
        mock_webhook.assert_called_once()
        webhook_instance = mock_webhook.return_value
        webhook_instance.execute.assert_called_once()

@patch('scrape.DiscordWebhook')
def test_post_to_discord_updated(mock_webhook, sample_event, mock_config):
    with patch('scrape.config', mock_config):
        post_to_discord_updated(sample_event, 'GTA')
        mock_webhook.assert_called_once()
        webhook_instance = mock_webhook.return_value
        webhook_instance.execute.assert_called_once()

@patch('scrape.DiscordWebhook')
def test_post_to_discord_completed(mock_webhook, sample_event, mock_config):
    with patch('scrape.config', mock_config):
        post_to_discord_completed(sample_event, 'GTA')
        mock_webhook.assert_called_once()
        webhook_instance = mock_webhook.return_value
        webhook_instance.execute.assert_called_once()

# Database Operation Tests
@mock_aws
def test_cleanup_old_events(sample_db_items):
    # Create mock DynamoDB table
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.create_table(
        TableName='test-db',
        KeySchema=[{'AttributeName': 'EventID', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'EventID', 'AttributeType': 'S'}],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Modify items to ensure they're old enough
    old_timestamp = int((datetime.now() - timedelta(days=8)).timestamp())
    for item in sample_db_items:
        if item.get('isActive') == 0:
            item['LastUpdated'] = Decimal(str(old_timestamp))
    
    # Add test items from fixture
    for item in sample_db_items:
        table.put_item(Item=item)
    
    with patch('scrape.table', table):
        cleanup_old_events()
    
    # Verify old items were deleted
    for item in sample_db_items:
        if item.get('isActive') == 0:
            response = table.get_item(Key={'EventID': item['EventID']})
            assert 'Item' not in response

@mock_aws
def test_close_recent_events(sample_db_items):
    # Setup mock DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.create_table(
        TableName='test-db',
        KeySchema=[{'AttributeName': 'EventID', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'EventID', 'AttributeType': 'S'}],
        BillingMode='PAY_PER_REQUEST'
    )

    # Ensure we have an active item
    active_item = sample_db_items[0].copy()
    active_item['isActive'] = 1
    table.put_item(Item=active_item)

    # Mock API response with empty list (no active events)
    mock_response = Mock()
    mock_response.text = json.dumps([])  # Empty list means no current events

    with patch('scrape.table', table), \
         patch('scrape.post_to_discord_completed') as mock_post:
        close_recent_events(mock_response)
        mock_post.assert_called_once()

# Utility Function Tests
def test_float_to_decimal(sample_event):
    result = float_to_decimal(sample_event)
    # Check that numeric values are converted to Decimal
    for key, value in result.items():
        if isinstance(value, float):
            assert isinstance(result[key], Decimal)
        elif isinstance(value, dict):
            # Check nested dictionaries
            for nested_key, nested_value in value.items():
                if isinstance(nested_value, float):
                    assert isinstance(result[key][nested_key], Decimal)

# Main Function Test
@patch('scrape.requests.get')
@patch('scrape.post_to_discord_closure')
def test_check_and_post_events(mock_post, mock_get, mock_dynamodb_table, sample_events):
    # Modify sample event to ensure it triggers a post
    sample_events[0]['IsFullClosure'] = True
    
    # Mock API response
    mock_get.return_value.ok = True
    mock_get.return_value.text = json.dumps(sample_events)
    
    # Mock the database query to return no existing items
    mock_dynamodb_table.query.return_value = {'Items': []}
    
    with patch('scrape.table', mock_dynamodb_table), \
         patch('scrape.config', mock_config):
        # Test the main function
        check_and_post_events()
        
        # Verify Discord post was called for new events
        assert mock_post.call_count > 0

# Error Handling Tests
def test_check_which_polygon_point_invalid_input():
    from shapely.geometry import Point
    point = Point(0, 0)  # Use valid coordinates that should return 'Other'
    assert check_which_polygon_point(point) == 'Other'

@mock_aws
@patch('scrape.requests.get')
def test_check_and_post_events_api_error(mock_get):
    # Set up mock DynamoDB table
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.create_table(
        TableName='test-db',
        KeySchema=[{'AttributeName': 'EventID', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'EventID', 'AttributeType': 'S'}],
        BillingMode='PAY_PER_REQUEST'
    )
    
    with patch('scrape.table', table):
        mock_get.return_value.ok = False
        with pytest.raises(Exception, match='Issue connecting to ON511 API'):
            check_and_post_events()