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

polygon_CentralOntario = Polygon([
    (43.69673616, -79.97053635),
    (43.38832077, -81.73173649),
    (43.71645113, -81.739295),
    (43.71589927, -81.75217319),
    (43.77013518, -81.7377233),
    (43.80358122, -81.73576572),
    (43.82147423, -81.72826167),
    (43.87018196, -81.73119804),
    (43.95617618, -81.72837716),
    (43.95536355, -81.72961525),
    (43.99225997, -81.74213795),
    (44.03075405, -81.73943605),
    (44.05398011, -81.75367101),
    (44.05771059, -81.75146604),
    (44.07157434, -81.76282259),
    (44.08501486, -81.75306706),
    (44.10171526, -81.73342237),
    (44.10551796, -81.7231963),
    (44.30047023, -81.60767854),
    (44.32989211, -81.62552319),
    (44.33445024, -81.60322005),
    (44.33675589, -81.5861354),
    (44.42462068, -81.53391396),
    (44.45597773, -81.41207514),
    (44.55535743, -81.35996158),
    (45.06491679, -81.58126228),
    (45.30888364, -81.84962809),
    (45.35263196, -81.58126228),
    (45.25415071, -81.18260292),
    (45.12120409, -81.18649228),
    (44.97281572, -81.09898169),
    (44.98382065, -80.95702006),
    (44.77850712, -80.78199888),
    (44.73271613, -80.59703225),
    (44.63476686, -80.55447134),
    (44.62857853, -80.45772949),
    (44.59600658, -80.45918203),
    (44.54131477, -80.24145688),
    (44.96939187, -80.25255188),
    (45.00156485, -79.9944078),
    (45.03031091, -80.02721268),
    (45.50897118, -76.48035557),
    (45.4923879, -76.42547571),
    (45.46607271, -76.3004954),
    (45.51243609, -76.25640548),
    (45.5238893, -76.14470942),
    (45.49429709, -76.01326221),
    (45.37864314, -75.8225616),
    (45.41738015, -75.73402203),
    (45.43458808, -75.72040056),
    (45.46564465, -75.68430366),
    (45.47042107, -75.62096382),
    (45.52675223, -75.41051208),
    (45.58588696, -75.25454622),
    (45.59589556, -75.04341341),
    (45.64400718, -74.98279786),
    (45.63734071, -74.80912409),
    (45.63972168, -74.65996897),
    (45.56824844, -74.37255591),
    (45.3087538, -74.47267373),
    (45.20165851, -74.29129532),
    (45.17540827, -74.31470722),
    (45.04247162, -74.55308298),
    (44.9980906, -74.66694997),
    (44.98981237, -74.74569911),
    (45.0055682, -74.77343423),
    (44.96675872, -74.95301419),
    (44.89141695, -75.16418691),
    (44.65178841, -75.57323014),
    (44.51624554, -75.75893589),
    (44.51091913, -75.73837121),
    (44.11879629, -76.359477),
    (44.06923212, -76.51779808),
    (43.80321178, -77.04959455),
    (43.85592561, -77.30128448),
    (43.96997917, -77.67475986),
    (43.89981823, -78.23497293),
    (43.87568031, -78.43468662),
    (44.02245114, -78.75180326),
    (44.007223, -79.28352851),
    (43.97844807, -79.69290989),
    (43.69673616, -79.97053635)
])

polygon_NorthernOntario = Polygon([
    (45.50897118, -76.48035557),
    (45.03031091, -80.02721268),
    (45.67526235, -80.76760838),
    (45.91014877, -81.15910959),
    (45.46493378, -81.54150611),
    (45.50323392, -82.32450851),
    (45.78330607, -83.52632615),
    (46.17557711, -84.09992094),
    (46.37067065, -84.0726069),
    (46.43973047, -84.47321278),
    (46.54628732, -84.67351572),
    (46.88960008, -84.82829527),
    (46.94704061, -84.97520889),
    (47.36040308, -84.89202802),
    (47.83572798, -86.36894955),
    (48.23225032, -86.20416613),
    (48.66131606, -86.75150551),
    (48.6693093, -87.70761166),
    (48.40486256, -88.3853578),
    (48.0986359, -88.96628306),
    (47.97725195, -89.42618223),
    (48.04202564, -90.0192101),
    (48.03393338, -91.84670414),
    (48.54126522, -93.32322251),
    (48.6293304, -94.8239461),
    (49.03562892, -95.199127),
    (52.85317039, -95.16281917),
    (56.98719545, -88.79684653),
    (56.82504535, -87.99720174),
    (56.12008621, -87.18719945),
    (55.11587993, -81.69416786),
    (53.53472463, -81.53528078),
    (52.84515204, -80.36579493),
    (52.40507281, -80.15558328),
    (51.61840375, -79.7363532),
    (51.60952138, -79.5384767),
    (48.64814624, -79.51767555),
    (47.57804507, -79.51939917),
    (47.55426886, -79.50178033),
    (47.47367381, -79.56104368),
    (47.40813306, -79.5442257),
    (47.32622799, -79.47855551),
    (47.23794303, -79.41368617),
    (47.1179115, -79.44772255),
    (47.02682267, -79.3556241),
    (46.9142471, -79.28594963),
    (46.83733025, -79.19305033),
    (46.76030315, -79.12537799),
    (46.69827251, -79.08933947),
    (46.59904018, -78.99443803),
    (46.51836596, -78.94318324),
    (46.44778107, -78.83426682),
    (46.4005785, -78.75015843),
    (46.40513397, -78.74115541),
    (46.32614733, -78.70043668),
    (46.29032909, -78.37320265),
    (46.28641476, -78.22156825),
    (46.20686308, -77.82853883),
    (46.20022857, -77.68954062),
    (46.17754454, -77.59470811),
    (46.05672749, -77.36631532),
    (46.00929494, -77.27010591),
    (45.9715134, -77.29771382),
    (45.92090389, -77.23608402),
    (45.84635994, -77.14015347),
    (45.79758053, -77.00462369),
    (45.79019188, -76.9611203),
    (45.80652338, -76.92765615),
    (45.85063339, -76.92765615),
    (45.89005017, -76.93602219),
    (45.90247134, -76.88331616),
    (45.87898523, -76.7787407),
    (45.84597143, -76.75671014),
    (45.7874695, -76.77595203),
    (45.73824923, -76.75782561),
    (45.72345522, -76.68922412),
    (45.64630721, -76.67974261),
    (45.56104817, -76.65352903),
    (45.53965981, -76.58195768),
    (45.50897118, -76.48035557)
])

# Define the coordinates of your polygon
polygon_SouthernOntario = Polygon([
    (43.38832077, -81.73173649),
    (43.69673616, -79.97053635),
    (43.4786045, -79.80584269),
    (43.40218933, -79.67253612),
    (43.3282737, -79.75987518),
    (43.2893307, -79.77481032),
    (43.22615524, -79.54903225),
    (43.22795231, -79.54804908),
    (43.20751926, -79.28143093),
    (43.23946611, -79.25186333),
    (43.27102503, -79.06324246),
    (43.14208748, -79.0250085),
    (42.97779925, -78.97951407),
    (42.95594173, -78.90732755),
    (42.88485117, -78.91106134),
    (42.87208264, -78.95711136),
    (42.84745016, -79.0479668),
    (42.82280786, -79.08779384),
    (42.84106236, -79.11393034),
    (42.86022379, -79.20851956),
    (42.82554638, -79.33920204),
    (42.83259314, -79.74276088),
    (42.82262391, -79.75024297),
    (42.77806155, -80.04986583),
    (42.74911794, -80.15099703),
    (42.75459089, -80.20548878),
    (42.65552884, -80.32118756),
    (42.55384468, -80.00381581),
    (42.51257409, -80.0834699),
    (42.5602621, -80.3859065),
    (42.56759548, -80.60619983),
    (42.63355714, -80.94348509),
    (42.62256838, -81.32184199),
    (42.57771545, -81.42374817),
    (42.56942869, -81.4214096),
    (42.36009566, -81.80847867),
    (42.31041428, -81.79478812),
    (42.24687541, -81.85701787),
    (42.23950444, -81.93418277),
    (42.18972786, -82.11713824),
    (42.19664486, -82.12549485),
    (42.09467489, -82.4058843),
    (42.01520098, -82.41584106),
    (41.88190576, -82.47682622),
    (41.90691985, -82.52163164),
    (41.91856509, -82.53024565),
    (41.98493634, -82.56955154),
    (42.01372886, -82.59606042),
    (42.01941005, -82.61441272),
    (42.02243981, -82.65366625),
    (42.01789511, -82.72503631),
    (41.98784806, -82.80394808),
    (41.98342945, -82.84006084),
    (41.98677888, -82.84045353),
    (41.97546227, -82.90091253),
    (41.99289347, -82.99369361),
    (42.01827385, -83.03345693),
    (42.02887758, -83.09514105),
    (42.05045827, -83.13439458),
    (42.14313393, -83.12521843),
    (42.21189103, -83.12980651),
    (42.26133475, -83.1338848),
    (42.31827906, -83.07016153),
    (42.33410873, -83.00643826),
    (42.34239887, -82.95291072),
    (42.34275877, -82.88230675),
    (42.35549718, -82.88380881),
    (42.31593637, -82.72450064),
    (42.3223782, -82.57141545),
    (42.33065958, -82.47558163),
    (42.36929161, -82.4307762),
    (42.42839159, -82.40600488),
    (42.46006798, -82.44442694),
    (42.44513556, -82.44131338),
    (42.52112393, -82.68173635),
    (42.55105748, -82.6689917),
    (42.59915585, -82.53596938),
    (42.63504543, -82.50835681),
    (42.64800549, -82.51064065),
    (42.67559664, -82.50574671),
    (42.76789259, -82.46482134),
    (42.76691981, -82.46659513),
    (42.80690622, -82.4845396),
    (42.85404262, -82.47051029),
    (42.88273753, -82.46855271),
    (42.92193232, -82.45843855),
    (42.95370005, -82.42711729),
    (42.98903134, -82.42581224),
    (43.00525783, -82.41928698),
    (43.01353714, -82.40106089),
    (43.0151212, -82.40215052),
    (43.17961302, -82.05117471),
    (43.21862623, -82.04121795),
    (43.22588175, -81.96405305),
    (43.37947593, -81.73153332),
    (43.38832077, -81.73173649)
])

# Load the configuration file
with open('config.json', 'r') as f:
    config = json.load(f)

DISCORD_WEBHOOK_URL = os.environ['DISCORD_WEBHOOK']
AWS_ACCESS_KEY_ID = os.environ['AWS_DB_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_DB_SECRET_ACCESS_KEY']

discordUsername = "ON511"
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

    # Perform API call to ON511 API
    response = requests.get("https://511on.ca/api/v2/get/event")
    if not response.ok:
        raise Exception('Issue connecting to ON511 API')

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
        markCompleted = False
        # If an item's ID is not in the set of active event IDs, mark it as closed
        if item['EventID'] not in active_event_ids:
            markCompleted = True
        else:
            # item exists, but now we need to check to see if it's no longer a full closure
            event = [x for x in data if x['ID']==item['EventID']]
            if event:
                if event[0]['IsFullClosure'] is False:
                    #now it's no longer a full closure - markt it as closed.
                    markCompleted = True
        # process relevant completions
        if markCompleted == True:
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

def generate_geojson():
    # Create a dictionary to store GeoJSON
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }

    # Define your polygons and their names
    polygons = {
        "GTA": polygon_GTA,
        "Central Ontario": polygon_CentralOntario,
        "Northern Ontario": polygon_NorthernOntario,
        "Southern Ontario": polygon_SouthernOntario
    }

    # Convert each polygon to GeoJSON format
    for name, polygon in polygons.items():
        feature = {
            "type": "Feature",
            "properties": {
                "name": name
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    list(map(lambda coord: [coord[1], coord[0]], polygon.exterior.coords))  # Convert (lat, lon) to [lon, lat]
                ]
            }
        }
        geojson["features"].append(feature)

    # Write GeoJSON to a file
    with open("polygons.geojson", "w") as f:
        json.dump(geojson, f, indent=2)

    print("GeoJSON saved as 'polygons.geojson'")

def lambda_handler(event, context):
    check_and_post_events()