import requests
import json
import time
import boto3
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.conditions import Key
from shapely.geometry import Point, Polygon
from decimal import Decimal
from discord_webhook import DiscordWebhook, DiscordEmbed
import os
from datetime import datetime, timedelta, date
import calendar
from pytz import timezone
import random

# Define the coordinates of your polygons
polygon_GTA = Polygon([
    (43.87568031, -78.43468662),
    (43.82078823, -78.88855484),
    (43.74310867, -79.1407252),
    (43.73611803, -79.13384386),
    (43.59837333, -79.36036016),
    (43.60378104, -79.43379127),
    (43.40218933, -79.67253612),
    (43.4786045, -79.80584269),
    (43.69673616, -79.97053635),
    (43.97844807, -79.69290989),
    (44.007223, -79.28352851),
    (44.02245114, -78.75180326),
    (43.87568031, -78.43468662)
])


# Load the configuration file
with open('config.json', 'r') as f:
    config = json.load(f)

DISCORD_WEBHOOK_URL = os.environ['DISCORD_WEBHOOK']
AWS_ACCESS_KEY_ID = os.environ['AWS_DB_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_DB_SECRET_ACCESS_KEY']

discordUsername = "SK511"
discordAvatarURL = "https://pbs.twimg.com/profile_images/1256233970905341959/EKlyRkOM_400x400.jpg"

# Create a DynamoDB resource object
dynamodb = boto3.resource('dynamodb',
    region_name='us-east-1',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

# Specify the name of your DynamoDB table
table = dynamodb.Table(config['db_name'])

# set the current UTC timestamp for use in a few places
utc_timestamp = calendar.timegm(datetime.utcnow().timetuple())

# Function to convert the float values in the event data to Decimal, as DynamoDB doesn't support float type
def float_to_decimal(event):
    for key, value in event.items():
        if isinstance(value, float):
            event[key] = Decimal(str(value))
        elif isinstance(value, dict):
            event[key] = float_to_decimal(value)
    return event

def check_which_polygon_point(point):
    # Function to see which polygon a point is in, and returns the text. Returns "Other" if unknown.
    try:
        if polygon_GTA.contains(point):
            return 'GTA'
        elif polygon_CentralOntario.contains(point):
            return 'Central Ontario'
        elif polygon_NorthernOntario.contains(point):
            return 'Northern Ontario'
        elif polygon_SouthernOntario.contains(point):
            return 'Southern Ontario'
        else:
            return 'Other'
    except:
        return 'Other'

def getThreadID(threadName):
    if threadName == 'GTA':
        return config['Thread-GTA']
    elif threadName == 'Central Ontario':
        return config['Thread-CentralOntario']
    elif threadName == 'Northern Ontario':
        return config['Thread-NorthernOntario']
    elif threadName == 'Southern Ontario':
        return config['Thread-SouthernOntario']
    else:
        return config['Thread-CatchAll'] #Other catch all thread

def unix_to_readable(unix_timestamp):
    utc_time = datetime.utcfromtimestamp(int(unix_timestamp))
    local_tz = timezone(config['timezone'])
    local_time = utc_time.replace(tzinfo=timezone('UTC')).astimezone(local_tz)
    return local_time.strftime('%Y-%b-%d %I:%M %p')

def post_to_discord_closure(event,threadName=None):
    # Create a webhook instance
    threadID = getThreadID(threadName)
    if threadID is not None:
        webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, username=discordUsername, avatar_url=discordAvatarURL, thread_id=threadID)
    else:
        webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, username=discordUsername, avatar_url=discordAvatarURL)

    #define type for URL
    if event['EventType'] == 'closures':
        URLType = 'Closures'
    elif event['EventType'] == 'accidentsAndIncidents':
        URLType = 'Incidents'
    else:
        URLType = 'Closures'


    urlWME = f"https://www.waze.com/en-GB/editor?env=usa&lon={event['Longitude']}&lat={event['Latitude']}&zoomLevel=15"
    url511 = f"https://511on.ca/map#{URLType}-{event['ID']}"
    urlLivemap = f"https://www.waze.com/live-map/directions?dir_first=no&latlng={event['Latitude']}%2C{event['Longitude']}&overlay=false&zoom=16"

    embed = DiscordEmbed(title=f"Closed", color=15548997)
    embed.add_embed_field(name="Road", value=event['RoadwayName'])
    embed.add_embed_field(name="Direction", value=event['DirectionOfTravel'])
    embed.add_embed_field(name="Information", value=event['Description'], inline=False)
    embed.add_embed_field(name="Start Time", value=unix_to_readable(event['StartDate']))
    if 'PlannedEndDate' in event and event['PlannedEndDate'] is not None:
        embed.add_embed_field(name="Planned End Time", value=unix_to_readable(event['PlannedEndDate']))
    embed.add_embed_field(name="Links", value=f"[511]({url511}) | [WME]({urlWME}) | [Livemap]({urlLivemap})", inline=False)
    embed.set_footer(text=config['license_notice'])
    embed.set_timestamp(datetime.utcfromtimestamp(int(event['StartDate'])))
    # Send the closure notification
    webhook.add_embed(embed)
    webhook.execute()

def post_to_discord_updated(event,threadName=None):
    # Function to post to discord that an event was updated (already previously reported)
    # Create a webhook instance
    threadID = getThreadID(threadName)
    if threadID is not None:
        webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, username=discordUsername, avatar_url=discordAvatarURL, thread_id=threadID)
    else:
        webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, username=discordUsername, avatar_url=discordAvatarURL)

    #define type for URL
    if event['EventType'] == 'closures':
        URLType = 'Closures'
    elif event['EventType'] == 'accidentsAndIncidents':
        URLType = 'Incidents'
    else:
        URLType = 'Closures'

    urlWME = f"https://www.waze.com/en-GB/editor?env=usa&lon={event['Longitude']}&lat={event['Latitude']}&zoomLevel=15"
    url511 = f"https://511on.ca/map#{URLType}-{event['ID']}"
    urlLivemap = f"https://www.waze.com/live-map/directions?dir_first=no&latlng={event['Latitude']}%2C{event['Longitude']}&overlay=false&zoom=16"

    embed = DiscordEmbed(title=f"Closure Update", color='ff9a00')
    embed.add_embed_field(name="Road", value=event['RoadwayName'])
    embed.add_embed_field(name="Direction", value=event['DirectionOfTravel'])
    embed.add_embed_field(name="Information", value=event['Description'], inline=False)
    embed.add_embed_field(name="Start Time", value=unix_to_readable(event['StartDate']))
    if 'PlannedEndDate' in event and event['PlannedEndDate'] is not None:
        embed.add_embed_field(name="Planned End Time", value=unix_to_readable(event['PlannedEndDate']))
    if 'Comment' in event and event['Comment'] is not None:
        embed.add_embed_field(name="Comment", value=event['Comment'], inline=False)
    embed.add_embed_field(name="Links", value=f"[511]({url511}) | [WME]({urlWME}) | [Livemap]({urlLivemap})", inline=False)
    embed.set_footer(text=config['license_notice'])
    embed.set_timestamp(datetime.utcfromtimestamp(int(event['LastUpdated'])))

    # Send the closure notification
    webhook.add_embed(embed)
    webhook.execute()

def post_to_discord_completed(event,threadName=None):
    # Create a webhook instance
    threadID = getThreadID(threadName)
    if threadID is not None:
        webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, username=discordUsername, avatar_url=discordAvatarURL, thread_id=threadID)
    else:
        webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, username=discordUsername, avatar_url=discordAvatarURL)

    urlWME = f"https://www.waze.com/en-GB/editor?env=usa&lon={event['Longitude']}&lat={event['Latitude']}&zoomLevel=15"
    urlLivemap = f"https://www.waze.com/live-map/directions?dir_first=no&latlng={event['Latitude']}%2C{event['Longitude']}&overlay=false&zoom=16"

    if 'lastTouched' in event:
        lastTouched = int(event['lastTouched'])
    else:
        lastTouched = utc_timestamp

    embed = DiscordEmbed(title=f"Cleared", color='34e718')
    embed.add_embed_field(name="Road", value=event['RoadwayName'])
    embed.add_embed_field(name="Direction", value=event['DirectionOfTravel'])
    embed.add_embed_field(name="Information", value=event['Description'], inline=False)
    embed.add_embed_field(name="Start Time", value=unix_to_readable(event['StartDate']))
    embed.add_embed_field(name="Ended", value=unix_to_readable(lastTouched))
    embed.add_embed_field(name="Links", value=f"[WME]({urlWME}) | [Livemap]({urlLivemap})", inline=False)
    embed.set_footer(text=config['license_notice'])
    embed.set_timestamp(datetime.utcfromtimestamp(lastTouched))

    # Send the closure notification
    webhook.add_embed(embed)
    webhook.execute()

def check_and_post_events():
    #check if we need to clean old events
    last_execution_day = get_last_execution_day()
    today = date.today().isoformat()
    if last_execution_day is None or last_execution_day < today:
        # Perform cleanup of old events
        cleanup_old_events()

        # Update last execution day to current date
        update_last_execution_day()

    # Perform API call to SK511 API
    response = requests.get("https://hotline.gov.sk.ca/api/v2/get/event")
    if not response.ok:
        raise Exception('Issue connecting to SK511 API')

    #use the response to close out anything recent
    close_recent_events(response)
    # Parse the response
    data = json.loads(response.text)

    # Iterate over the events
    for event in data:
        # Check if the event is a full closure
        if event['IsFullClosure']:
            # Create a point from the event's coordinates
            point = Point(event['Latitude'], event['Longitude'])
            # Try to get the event with the specified ID and isActive=1 from the DynamoDB table
            dbResponse = table.query(
                KeyConditionExpression=Key('EventID').eq(event['ID']),
                FilterExpression=Attr('isActive').eq(1)
            )
            #If the event is not in the DynamoDB table
            if not dbResponse['Items']:
                # Set the EventID key in the event data
                event['EventID'] = event['ID']
                # Set the isActive attribute
                event['isActive'] = 1
                # set LastTouched
                event['lastTouched'] = utc_timestamp
                event['DetectedPolygon'] = check_which_polygon_point(point)
                # Convert float values in the event to Decimal
                event = float_to_decimal(event)
                # If the event is within the specified area and has not been posted before, post it to Discord
                post_to_discord_closure(event,event['DetectedPolygon'])
                # Add the event ID to the DynamoDB table
                table.put_item(Item=event)
            else:
                # We have seen this event before
                # First, let's see if it has a lastupdated time
                event = float_to_decimal(event)
                lastUpdated = dbResponse['Items'][0].get('LastUpdated')
                if lastUpdated != None:
                    # Now, see if the version we stored is different
                    if lastUpdated != event['LastUpdated']:
                        # Store the most recent updated time:
                        event['EventID'] = event['ID']
                        event['isActive'] = 1
                        event['lastTouched'] = utc_timestamp
                        event['DetectedPolygon'] = check_which_polygon_point(point)
                        # It's different, so we should fire an update notification
                        post_to_discord_updated(event,event['DetectedPolygon'])
                        table.put_item(Item=event)
                # get the lasttouched time
                lastTouched_datetime = datetime.fromtimestamp(int(dbResponse['Items'][0].get('lastTouched')))
                # store the current time now
                now = datetime.fromtimestamp(utc_timestamp)
                # Compute the difference in minutes between now and lastUpdated
                time_diff_min = (now - lastTouched_datetime).total_seconds() / 60
                # Compute the variability
                variability = random.uniform(-2, 2)  # random float between -2 and 2
                # Add variability to the time difference
                time_diff_min += variability
                # If time_diff_min > 5, then more than 5 minutes have passed (considering variability)
                if abs(time_diff_min) > 5:
                    # let's store that we just saw it to keep track of the last touch time
                    table.update_item(
                        Key={'EventID': event['ID']},
                        UpdateExpression="SET lastTouched = :val",
                        ExpressionAttributeValues={':val': utc_timestamp}
                    )

def close_recent_events(responseObject):
    #function uses the API response from ON511 to determine what we stored in the DB that can now be closed
    #if it finds a closure no longer listed in the response object, then it marks it closed and posts to discord
    data = json.loads(responseObject.text)

    # Create a set of active event IDs
    active_event_ids = {event['ID'] for event in data}

    # Get the list of event IDs in the table
    response = table.scan(
        FilterExpression=Attr('isActive').eq(1)
    )

    # Iterate over the items
    for item in response['Items']:
        # If an item's ID is not in the set of active event IDs, mark it as closed
        if item['EventID'] not in active_event_ids:
            # Convert float values in the item to Decimal
            item = float_to_decimal(item)
            # Remove the isActive attribute from the item
            table.update_item(
                Key={'EventID': item['EventID']},
                UpdateExpression="SET isActive = :val",
                ExpressionAttributeValues={':val': 0}
            )
            # Notify about closure on Discord
            if 'DetectedPolygon' in item and item['DetectedPolygon'] is not None:
                post_to_discord_completed(item,item['DetectedPolygon'])
            else:
                post_to_discord_completed(item)

def cleanup_old_events():
    # Get the current time and subtract 5 days to get the cut-off time
    now = datetime.now()
    cutoff = now - timedelta(days=5)
    # Convert the cutoff time to Unix timestamp
    cutoff_unix = Decimal(str(cutoff.timestamp()))
    # Initialize the scan parameters
    scan_params = {
        'FilterExpression': Attr('LastUpdated').lt(cutoff_unix) & Attr('isActive').eq(0)
    }
    while True:
        # Perform the scan operation
        response = table.scan(**scan_params)
        # Iterate over the matching items and delete each one
        for item in response['Items']:
            table.delete_item(
                Key={
                    'EventID': item['EventID']
                }
            )
        # If the scan returned a LastEvaluatedKey, continue the scan from where it left off
        if 'LastEvaluatedKey' in response:
            scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
        else:
            # If no LastEvaluatedKey was returned, the scan has completed and we can break from the loop
            break

def get_last_execution_day():
    response = table.query(
        KeyConditionExpression=Key('EventID').eq('LastCleanup')
    )

    items = response.get('Items')
    if items:
        item = items[0]
        last_execution_day = item.get('LastExecutionDay')
        return last_execution_day

    return None

def update_last_execution_day():
    today = datetime.now().date().isoformat()
    table.put_item(
        Item={
            'EventID': 'LastCleanup',
            'LastExecutionDay': today
        }
    )

def lambda_handler(event, context):
    check_and_post_events()