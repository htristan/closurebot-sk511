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
import logging
import random

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()  # Logs to the console
    ]
)

# Define the coordinates of your polygons
polygon_GTA = Polygon([
    (43.90145674, -78.43244733),
    (43.79786638, -79.08267715),
    (43.61039879, -79.32047943),
    (43.58807669, -79.46354826),
    (43.30149607, -79.7907069),
    (43.29630564, -79.80114458),
    (43.28310929, -79.87469002),
    (43.30924714, -79.91109858),
    (43.34707576, -79.86013592),
    (43.56097914, -80.1602388),
    (43.73409481, -79.96862481),
    (43.86457041, -80.14402135),
    (43.91420474, -80.06912728),
    (43.92477610, -80.06360594),
    (44.01561954, -79.65682927),
    (44.07995669, -79.55603431),
    (44.20679254, -79.51347862),
    (44.38843012, -79.49815149),
    (44.51587741, -79.08172744),
    (44.20420133, -78.94060988),
    (44.22702113, -78.76976192),
    (44.05672093, -78.69165063),
    (44.09673133, -78.51528441),
    (43.90145674, -78.43244733)
])

polygon_Central_EasternOntario = Polygon([
    (43.90145674, -78.43244733),
    (43.9682353, -77.74335735),
    (43.89849339, -77.54318752),
    (43.76422262, -76.713527),
    (44.09458945, -76.44095168),
    (44.13510045, -76.35358521),
    (44.19963597, -76.31049296),
    (44.20785809, -76.22249608),
    (44.23421321, -76.16598337),
    (44.28188959, -76.16178137),
    (44.30019222, -76.09779762),
    (44.33039824, -76.04816197),
    (44.34776994, -75.9992853),
    (44.34529861, -75.97439312),
    (44.34216449, -75.97211281),
    (44.349921, -75.94782012),
    (44.3709868, -75.90899723),
    (44.41846959, -75.8405627),
    (44.49518299, -75.79067503),
    (44.77646497, -75.39741723),
    (44.7871975, -75.36628898),
    (44.8593498, -75.26047903),
    (44.90329356, -75.13712814),
    (44.96402611, -75.0026269),
    (44.98507146, -74.97585243),
    (44.99404846, -74.89870228),
    (45.0171858, -74.81778388),
    (44.99020279, -74.7614873),
    (44.99304229, -74.72674212),
    (45.00274967, -74.67178744),
    (45.04225969, -74.60350749),
    (45.04532562, -74.53926485),
    (45.0809448, -74.4780385),
    (45.20535182, -74.34324025),
    (45.3030077, -74.47127449),
    (45.56314833, -74.3759117),
    (45.64579614, -74.63964168),
    (45.64363736, -74.94624811),
    (45.59673628, -75.0316854),
    (45.57880482, -75.15387289),
    (45.58792774, -75.23263086),
    (45.52971137, -75.38099733),
    (45.50575795, -75.5146572),
    (45.45482647, -75.69449208),
    (45.42403822, -75.70630887),
    (45.41782849, -75.74955322),
    (45.37229748, -75.80896544),
    (45.38849454, -75.87505043),
    (45.48061640, -75.985986),
    (45.52101651, -76.11470703),
    (45.51484855, -76.23485775),
    (45.47349435, -76.23962648),
    (45.46149579, -76.37666196),
    (45.52202993, -76.50160707),
    (45.53526968, -76.61278538),
    (45.576416, -76.66910094),
    (45.62654512, -76.67059441),
    (45.66369634, -76.71080183),
    (45.68194077, -76.68807737),
    (45.68922654, -76.6869226),
    (45.72323042, -76.69825611),
    (45.72426772, -76.74805631),
    (45.73365775, -76.76940249),
    (45.74755122, -76.76588513),
    (45.85285022, -76.76692738),
    (45.89431493, -76.80772131),
    (45.89762552, -76.92371687),
    (45.84547227, -76.90066242),
    (45.78827412, -76.93031308),
    (45.81355926, -77.05659312),
    (45.84105342, -77.11163013),
    (45.85704666, -77.16074012),
    (45.67664243, -77.84004399),
    (45.56017205, -78.31888496),
    (45.41324403, -78.85936743),
    (45.52165273, -78.91653691),
    (45.32050298, -79.66488598),
    (45.22616819, -79.61820412),
    (45.08604673, -80.12593896),
    (44.87273082, -80.25800192),
    (44.79131093, -80.23384437),
    (44.69981959, -80.01794039),
    (44.5505105, -80.00761376),
    (44.4771561, -80.09956864),
    (44.54077771, -80.26499668),
    (44.53303033, -80.29344599),
    (44.25409503, -80.22652416),
    (44.29489577, -80.03400006),
    (43.94999775, -79.95088107),
    (44.01561954, -79.65682927),
    (44.07995669, -79.55603431),
    (44.20679254, -79.51347862),
    (44.38843012, -79.49815149),
    (44.51587741, -79.08172744),
    (44.20420133, -78.94060988),
    (44.22702113, -78.76976192),
    (44.05672093, -78.69165063),
    (44.09673133, -78.51528441),
    (43.90145674, -78.43244733)
])

polygon_NorthernOntario = Polygon([
    (45.85704666, -77.16074012),
    (45.87180701, -77.19896427),
    (45.92415141, -77.24252696),
    (45.93492673, -77.27145704),
    (45.98923105, -77.28908094),
    (46.01306988, -77.27782274),
    (46.16626774, -77.58106438),
    (46.18559795, -77.65075134),
    (46.2000351, -77.67988445),
    (46.18918376, -77.68546798),
    (46.18378536, -77.69017383),
    (46.18249302, -77.69754648),
    (46.19120682, -77.72913646),
    (46.20786172, -77.87120222),
    (46.21880767, -77.89900931),
    (46.24567351, -77.99327665),
    (46.2469366, -78.07264797),
    (46.27412385, -78.13776047),
    (46.27415409, -78.18881632),
    (46.26976791, -78.22596515),
    (46.27475676, -78.24824274),
    (46.25434637, -78.31485297),
    (46.26999761, -78.35273974),
    (46.2916036, -78.38812017),
    (46.29552957, -78.4497832),
    (46.29489735, -78.50898688),
    (46.31862699, -78.59162103),
    (46.32084483, -78.64569848),
    (46.31961513, -78.70043869),
    (46.33780442, -78.72955988),
    (46.37528776, -78.72365035),
    (46.38761997, -78.73810733),
    (46.39840522, -78.76843545),
    (46.42273362, -78.80160542),
    (46.45171295, -78.87719316),
    (46.49488231, -78.93179118),
    (46.51448527, -78.94825632),
    (46.54652909, -78.9923671),
    (46.59243938, -78.9979498),
    (46.63894201, -79.02597088),
    (46.64662332, -79.04705544),
    (46.66693337, -79.07440225),
    (46.68896247, -79.09373186),
    (46.71148561, -79.10042259),
    (46.73394484, -79.12037611),
    (46.7863795, -79.14415126),
    (46.82572177, -79.1731614),
    (46.8335523, -79.21448902),
    (46.93940595, -79.3245504),
    (47.01289054, -79.35624323),
    (47.07424792, -79.43417268),
    (47.11318997, -79.45246158),
    (47.23871139, -79.43167147),
    (47.27552015, -79.44924003),
    (47.31914605, -79.50709994),
    (47.46569698, -79.61532601),
    (47.53661139, -79.51529),
    (51.46147834, -79.51278611),
    (51.4890891, -79.54537894),
    (51.47679914, -79.58517093),
    (51.36340442, -80.36214437),
    (51.72282672, -80.56685637),
    (52.04996478, -81.00478558),
    (52.21204113, -81.41351941),
    (52.45074386, -81.55304502),
    (52.99836909, -82.22409419),
    (53.14431234, -82.24354392),
    (53.27484402, -82.08813177),
    (53.62820265, -82.17576634),
    (53.79507854, -82.08651939),
    (54.47280628, -82.31905227),
    (54.81592358, -82.17795092),
    (55.1619245, -82.25610144),
    (55.30867865, -83.98734210),
    (55.28122213, -85.00921928),
    (55.67249204, -85.89544231),
    (56.105768, -87.62598932),
    (56.47708455, -87.9213178),
    (56.70454277, -88.64590823),
    (56.86018923, -88.81835907),
    (56.85655315, -88.97279384),
    (56.29467079, -89.90636709),
    (55.60520812, -90.98020934),
    (55.60580463, -90.98580597),
    (55.14314756, -91.67995629),
    (54.55265201, -92.5179867),
    (54.15146222, -93.09100621),
    (53.74983058, -93.63484858),
    (53.26728408, -94.46345719),
    (52.84077417, -95.15555545),
    (49.38341344, -95.15615858),
    (49.36039893, -95.07672307),
    (49.35951718, -94.94042044),
    (49.31252921, -94.82258161),
    (48.77578113, -94.69137049),
    (48.74260766, -94.64218903),
    (48.71702851, -94.58773475),
    (48.71420401, -94.54758269),
    (48.70183893, -94.53732496),
    (48.69252136, -94.44186267),
    (48.71066640, -94.41432807),
    (48.70437947, -94.344896),
    (48.7073716, -94.28568224),    
    (48.69210193, -94.25732992),
    (48.65713598, -94.25813371),
    (48.64788322, -94.22061379),
    (48.62677738, -93.83503227),
    (48.58502331, -93.81223623),
    (48.53007117, -93.81927644),
    (48.51560015, -93.79208666),
    (48.51683882, -93.64570537),
    (48.53519648, -93.51245079),
    (48.54937246, -93.45729945),
    (48.59121729, -93.46081578),
    (48.60845786, -93.36254791),
    (48.62144642, -93.34262833),
    (48.62480857, -93.00972822),
    (48.51563871, -92.62562995),
    (48.49251840, -92.70454934),
    (48.452322, -92.71598124),
    (48.43673538, -92.65484431),
    (48.44799479, -92.50995804),
    (48.41040098, -92.46471663),
    (48.2201932, -92.37030516),
    (48.24941564, -92.2664131),
    (48.28733983, -92.30249034),
    (48.35055412, -92.27847481),
    (48.34502197, -92.2148577),
    (48.3562292, -92.06385419),
    (48.30938072, -92.01304149),
    (48.26436626, -92.00768442),
    (48.23040465, -91.94417691),
    (48.23818108, -91.89936451),
    (48.20799089, -91.86396038),
    (48.20990735, -91.81735297),
    (48.19643333, -91.78705964),
    (48.20033195, -91.71338389),
    (48.16972186, -91.72423018),
    (48.17342242, -91.70893244),
    (48.11398003, -91.71198973),
    (48.10707379, -91.67239811),
    (48.10931041, -91.65237882),
    (48.09714222, -91.64069971),
    (48.1076641, -91.56305528),
    (48.06254427, -91.58003384),
    (48.04276675, -91.57143841),
    (48.06493682, -91.502902),
    (48.06923375, -91.44760369),
    (48.04807569, -91.43731563),
    (48.0573181, -91.38971856),
    (48.07226216, -91.32154282),
    (48.07401899, -91.28523218),
    (48.08394107, -91.24846104),
    (48.17959504, -91.08303412),
    (48.19039437, -91.02320258),
    (48.22365418, -90.96178717),
    (48.24324988, -90.84619256),
    (48.17622561, -90.83525302),
    (48.15924731, -90.79519421),
    (48.16175732, -90.77589433),
    (48.13495056, -90.79895708),
    (48.12347587, -90.77635661),
    (48.09274691, -90.75792811),
    (48.11566867, -90.58934762),
    (48.09673975, -90.56127678),
    (48.09989072, -90.48556981),
    (48.10605071, -90.45802289),
    (48.09401936, -90.34693838),
    (48.10280184, -90.32062708),
    (48.10840403, -90.15560128),
    (48.10064217, -90.06752673),
    (48.08021653, -90.02022949),
    (48.02707848, -89.99512725),
    (48.00957688, -89.95356311),
    (48.01576072, -89.93819496),
    (47.99596799, -89.92318479),
    (47.98408371, -89.88669807),
    (47.98572795, -89.86976337),
    (47.9934307, -89.84535642),
    (48.01416739, -89.80086057),
    (48.01993088, -89.74028963),
    (48.00591512, -89.70763427),
    (48.01049789, -89.67479313),
    (48.00621847, -89.66728315),
    (48.01026996, -89.65949708),
    (48.00287055, -89.65145954),
    (48.00398913, -89.63692765),
    (48.01246377, -89.61448683),
    (48.00618321, -89.60715659),
    (48.00173151, -89.58580496),
    (47.99628289, -89.58518616),
    (47.99840189, -89.56983656),
    (48.34611851, -88.56285588),
    (48.72864132, -87.88839344),
    (48.73588263, -87.36553352),
    (48.61212448, -86.99361625),
    (48.70826487, -86.43582863),
    (47.32842118, -85.8263845),
    (46.45293838, -84.49709055),
    (46.51189276, -84.37522433),
    (46.48913559, -84.28938615),
    (46.50271301, -84.2490136),
    (46.53767625, -84.21757133),
    (46.52847905, -84.16561649),
    (46.53058288, -84.12914811),
    (46.4735161, -84.09010622),
    (46.39310574, -84.14188615),
    (46.23285619, -84.10616896),
    (46.05532743, -83.9614014),
    (46.14510892, -83.59667585),
    (45.99142004, -83.44480264),
    (45.86990993, -83.51805543),
    (45.80884979, -83.10344184),
    (45.63485176, -82.95706216),
    (45.67957215, -82.35832834),
    (45.39933381, -81.78804059),
    (45.54508865, -81.42897999),
    (45.81675328, -81.45094194),
    (45.91726853, -81.16119578),
    (45.84933852, -80.7966804),
    (45.37670956, -80.44324123),
    (45.08604673, -80.12593896),
    (45.22616819, -79.61820412),
    (45.32050298, -79.66488598),
    (45.52165273, -78.91653691),
    (45.41324403, -78.85936743),
    (45.56017205, -78.31888496),
    (45.67664243, -77.84004399),
    (45.85704666, -77.16074012)
])

polygon_SouthernOntario = Polygon([
    (43.30149607, -79.7907069),
    (43.29630564, -79.80114458),
    (43.28310929, -79.87469002),
    (43.30924714, -79.91109858),
    (43.34707576, -79.86013592),
    (43.56097914, -80.1602388),
    (43.73409481, -79.96862481),
    (43.86457041, -80.14402135),
    (43.91420474, -80.06912728),
    (43.92477610, -80.06360594),
    (43.94999775, -79.95088107),
    (44.29489577, -80.03400006),
    (44.25409503, -80.22652416),
    (44.53303033, -80.29344599),
    (44.6257338, -80.59345711),
    (44.73192719, -80.64117897),
    (44.72682654, -80.75906082),
    (44.65704646, -80.88204654),
    (44.95381796, -80.88644185),
    (45.0187308, -81.23326028),
    (45.25182242, -81.28755974),
    (45.34936884, -81.66052755),
    (45.31466988, -81.8369315),
    (44.81088324, -81.35831456),
    (44.59198567, -81.31308351),
    (44.43264505, -81.43798826),
    (44.40477215, -81.53538509),
    (44.31251757, -81.61903441),
    (44.17179205, -81.65524134),
    (44.07289406, -81.76627689),
    (43.38661263, -81.71773634),
    (43.30347362, -81.78516011),
    (43.22490798, -81.96472833),
    (43.21624186, -82.02635497),
    (43.17939573, -82.02280195),
    (43.10511787, -82.11545723),
    (43.09248853, -82.15301792),
    (43.05545606, -82.1853936),
    (43.00961190, -82.41746478),
    (42.99276463, -82.42547627),
    (42.97164797, -82.41188245),
    (42.93276025, -82.45197651),
    (42.88399955, -82.47196302),
    (42.85278221, -82.4673512),
    (42.80487358, -82.48000794),
    (42.77192575, -82.46600127),
    (42.65656744, -82.51096571),
    (42.61831518, -82.51469199),
    (42.56366664, -82.58022352),
    (42.55120321, -82.58672556),
    (42.5482176, -82.60721857),
    (42.55702967, -82.62332587),
    (42.55488004, -82.6429811),
    (42.52268234, -82.67983044),
    (42.52807364, -82.61992391),
    (42.49117309, -82.59854072),
    (42.47451861, -82.52617235),
    (42.48342239, -82.42360562),
    (42.3665163, -82.42241631),
    (42.32233183, -82.45341311),
    (42.30759004, -82.50810621),
    (42.31807958, -82.559654),
    (42.30363095, -82.62509599),
    (42.30218049, -82.75801517),
    (42.33924618, -82.90833347),
    (42.35076173, -82.92692202),
    (42.34312659, -82.94938034),
    (42.33491625, -82.9674123),
    (42.31823292, -83.06374759),
    (42.3086738, -83.0798882),
    (42.28902821, -83.0978895),
    (42.23740865, -83.12697913),
    (42.05040021, -83.12439844),
    (41.97722648, -82.92099802),
    (41.73955157, -82.84131355),
    (41.71821644, -82.63982033),
    (42.14945428, -82.27878801),
    (42.24601073, -82.07424701),
    (42.25140971, -81.8385847),
    (42.62962534, -81.34781969),
    (42.63309584, -80.7916131),
    (42.57038943, -80.59380646),
    (42.57679412, -80.40207155),
    (42.5339373, -80.12586155),
    (42.54972479, -80.03377214),
    (42.61515161, -80.32019164),
    (42.68924498, -80.32145434),
    (42.77581002, -80.22106272),
    (42.78951895, -79.98267301),
    (42.82984832, -79.79082175),
    (42.83884608, -79.47939829),
    (42.83554159, -79.09303037),
    (42.88924593, -78.9148311),
    (42.90870589, -78.90432637),
    (42.93879103, -78.91543073),
    (42.95138286, -78.94014709),
    (42.9725914, -78.99541888),
    (42.98283692, -79.02269921),
    (43.01622280, -79.02924649),
    (43.05942459, -78.99978373),
    (43.07952252, -79.07633981),
    (43.0925904, -79.06502278),
    (43.10803274, -79.05765785),
    (43.11361277, -79.05951343),
    (43.12058903, -79.06897481),
    (43.1288969, -79.05447791),
    (43.1405417, -79.04236954),
    (43.15549611, -79.04429451),
    (43.17029199, -79.05180923),
    (43.20253883, -79.04963462),
    (43.21500394, -79.05543347),
    (43.25529145, -79.05607647),
    (43.26720287, -79.08762152),
    (43.24466029, -79.22151908),
    (43.19725608, -79.29784304),
    (43.20019429, -79.51974998),
    (43.26665885, -79.76388535),
    (43.30149607, -79.7907069)
])

# Load the configuration file
with open('config.json', 'r') as f:
    config = json.load(f)

DISCORD_WEBHOOK_URL = os.environ['DISCORD_WEBHOOK']
AWS_ACCESS_KEY_ID = os.environ.get('AWS_DB_KEY', None)
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_DB_SECRET_ACCESS_KEY', None)

discordUsername = "ON511"
discordAvatarURL = "https://pbs.twimg.com/profile_images/1256233970905341959/EKlyRkOM_400x400.jpg"

# Fallback mechanism for credentials
try:
    # Use environment variables if they exist
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        dynamodb = boto3.resource(
            'dynamodb',
            region_name='us-east-1',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    else:
        # Otherwise, use IAM role permissions (default behavior of boto3)
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
except (NoCredentialsError, PartialCredentialsError):
    print("AWS credentials are not properly configured. Ensure IAM role or environment variables are set.")
    raise

# Specify the name of your DynamoDB table
table = dynamodb.Table(config['db_name'])

utc_timestamp = None

def update_utc_timestamp():
    global utc_timestamp
    utc_timestamp = calendar.timegm(datetime.utcnow().timetuple())

# set the current UTC timestamp for use in a few places
update_utc_timestamp()


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
        elif polygon_Central_EasternOntario.contains(point):
            return 'Central & Eastern Ontario'
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
    elif threadName == 'Central & Eastern Ontario':
        return config['Thread-Central_EasternOntario']
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
                FilterExpression=Attr('isActive').eq(1),
                ConsistentRead=True
            )
            #If the event is not in the DynamoDB table
            update_utc_timestamp()
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
                # Get the lastTouched time
                lastTouched = dbResponse['Items'][0].get('lastTouched')
                if lastTouched is None:
                    logging.warning(f"EventID: {event['ID']} - Missing lastTouched. Setting it now.")
                    lastTouched_datetime = now
                else:
                    lastTouched_datetime = datetime.fromtimestamp(int(lastTouched))
                # store the current time now
                now = datetime.fromtimestamp(utc_timestamp)
                # Compute the difference in minutes between now and lastUpdated
                time_diff_min = (now - lastTouched_datetime).total_seconds() / 60
                # Compute the variability
                variability = random.uniform(-2, 2)  # random float between -2 and 2
                # Add variability to the time difference
                time_diff_min += variability
                # Log calculated time difference and variability
                logging.info(
                    f"EventID: {event['ID']}, TimeDiff: {time_diff_min:.2f} minutes (Variability: {variability:.2f}), LastTouched: {lastTouched_datetime}, Now: {now}"
                )
                # If time_diff_min > 5, then more than 5 minutes have passed (considering variability)
                if abs(time_diff_min) > 5:
                    logging.info(f"EventID: {event['ID']} - Updating lastTouched to {utc_timestamp}.")
                    response = table.update_item(
                        Key={'EventID': event['ID']},
                        UpdateExpression="SET lastTouched = :val",
                        ExpressionAttributeValues={':val': utc_timestamp}
                    )
                    logging.info(f"Update response for EventID {event['ID']}: {response}")
                    logging.info(f"EventID: {event['ID']} - lastTouched updated successfully.")
                # else:
                #     logging.info(f"EventID: {event['ID']} - No update needed. TimeDiff: {time_diff_min:.2f}")

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

if __name__ == "__main__":
    # Simulate the Lambda environment by passing an empty event and context
    event = {}
    context = None
    lambda_handler(event, context)
