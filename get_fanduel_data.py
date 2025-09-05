import pandas as pd
import requests
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
import os
from dotenv import load_dotenv

url = "https://api.sportsbook.fanduel.com/ips/stats/eventIds"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)
ids = response.content
ids = json.loads(ids)

game_id = ids[1]
url = f"https://fdx-api.sportsbook.fanduel.com/api/v1/event-page/{game_id}/initial-state/IN?flags=useCombinedTouchdownsVirtualMarket%2CusePulse%2CuseQuickBets%2CuseQuickBetsNFL%2CuseQuickBetsMLB&channel=WEB"
response = requests.request("GET", url, headers=headers, data=payload)

game_line = json.loads(response.content)
game_lines = []
game_lines.append(game_line)


game_lines = []
for game_id in ids:
    url = f"https://fdx-api.sportsbook.fanduel.com/api/v1/event-page/{game_id}/initial-state/IN?flags=useCombinedTouchdownsVirtualMarket%2CusePulse%2CuseQuickBets%2CuseQuickBetsNFL%2CuseQuickBetsMLB&channel=WEB"
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        line = json.loads(response.content)
        game_lines.append(line)
    except Exception as e:
        print('Could not retrieve lines: {e}')


load_dotenv()
# mongo uri
uri = os.getenv("mongo_uri")
client = MongoClient(uri, server_api=ServerApi('1'))
db = client["Fanduel"]
collection = db["Lines"]


# Push raw line data to mongo
today = datetime.utcnow().strftime("%Y-%m-%d")
for line in game_lines:
    line["timestamp"] = datetime.utcnow()
    line["date"] = today
    line["game_id"] = line.get('eventInfo').get('eventId')
    
    # Only insert if a row for this game_id + date doesn't exist
    exists = collection.find_one({"game_id": line["game_id"], "date": today})
    if not exists:
        collection.insert_one(line)


# push simplified line data to mongo
simple_game_lines = []
for line in game_lines:
    simplified_lines = []
    markets = line.get("attachments", {}).get("markets", {})
    if markets:
        for market_id, market_data in markets.items():
            if market_data.get('marketName') in ['Moneyline', 'Spread', 'Total Match Points']:
                simple_line = {
                    "market_name": market_data.get('marketName'),
                    "runners": market_data.get('runners')
                }
                simplified_lines.append(simple_line)
        simple_game_line = {
            "game_id": line.get('eventInfo').get('eventId'),
            "lines": simplified_lines
        }
        simple_game_lines.append(simple_game_line)
        

collection = db["SimpleLines"]
today = datetime.utcnow().strftime("%Y-%m-%d")
for line in simple_game_lines:
    line["timestamp"] = datetime.utcnow()
    line["date"] = today
    
    # Only insert if a row for this game_id + date doesn't exist
    exists = collection.find_one({"game_id": line["game_id"], "date": today})
    if not exists:
        collection.insert_one(line)
