import os
import time
import json
import requests
from dotenv import load_dotenv
from google.cloud import pubsub_v1

# .env
load_dotenv()

# import variables from .env
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_ID = os.getenv("GCP_TOPIC_ID")
OPENSKY_URL = os.getenv("OPENSKY_URL")

# the service account key file for authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

# Publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def fetch_and_publish():
    print(f" Starting Producer for Project: {PROJECT_ID}...")
    while True:
        try:
            response = requests.get(OPENSKY_URL)
            if response.status_code == 200:
                data = response.json()
                states = data.get("states", [])
                
                if states:
                    
                    for flight in states[:10]:
                        flight_data = {
                            "icao24": flight[0],
                            "origin_country": flight[2],
                            "longitude": flight[5],
                            "latitude": flight[6],
                            "velocity": flight[9],
                            "timestamp": int(time.time())
                        }
                        
                        data_str = json.dumps(flight_data)
                        publisher.publish(topic_path, data_str.encode("utf-8"))
                        
                    print(f"✅ Published {len(states[:10])} flights to {TOPIC_ID}")
            
            time.sleep(15) # OpenSky Free Tier 
            
        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    fetch_and_publish()