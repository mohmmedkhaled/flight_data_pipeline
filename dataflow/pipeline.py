import os
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from dotenv import load_dotenv
import psycopg2 

load_dotenv()

# use service account key for authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

# data parsing and filtering
class ParseAndFilterFn(beam.DoFn):
    def process(self, element):
        try:
            data = json.loads(element.decode('utf-8'))
            if data.get('icao24') and data.get('latitude') and data.get('longitude'):
                yield data
        except Exception as e:
            logging.error(f"Parsing error: {e}")

# data mapping for PostgreSQL insertion
class PrepareForPostgresFn(beam.DoFn):
    def process(self, element):
        yield {
            "icao24": element.get("icao24"),
            "callsign": element.get("callsign"),
            "origin_country": element.get("origin_country"),
            "time_position": element.get("timestamp"),
            "longitude": float(element.get("longitude")),
            "latitude": float(element.get("latitude")),
            "velocity": float(element.get("velocity")) if element.get("velocity") else None
        }

# writing to PostgreSQL
class WriteToPostgresFn(beam.DoFn):
    def __init__(self):
        # initialize database connection parameters from environment variables
        self.host = os.getenv("DB_HOST")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASS")
        self.dbname = os.getenv("DB_NAME")

    def process(self, element):
        try:
           
            conn = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                dbname=self.dbname
            )
            cur = conn.cursor()
            
            insert_query = """
            INSERT INTO flight_data (icao24, callsign, origin_country, time_position, longitude, latitude, velocity)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            cur.execute(insert_query, (
                element['icao24'], 
                element['callsign'], 
                element['origin_country'],
                element['time_position'], 
                element['longitude'], 
                element['latitude'], 
                element['velocity']
            ))
            
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logging.error(f"Database Insert Error: {e}")

def run():
    options = PipelineOptions(
        streaming=True,
        project=os.getenv("GCP_PROJECT_ID"),
        region=os.getenv("GCP_REGION", "asia-south1"),
        temp_location=f"gs://{os.getenv('GCP_BUCKET')}/temp",
        staging_location=f"gs://{os.getenv('GCP_BUCKET')}/staging"
    )

    with beam.Pipeline(options=options) as p:
        (
            p 
            | "Read From PubSub" >> beam.io.ReadFromPubSub(subscription=os.getenv("GCP_SUBSCRIPTION_PATH"))
            | "Parse & Filter"   >> beam.ParDo(ParseAndFilterFn())
            | "Prepare Data"     >> beam.ParDo(PrepareForPostgresFn())
            | "Write to DB"      >> beam.ParDo(WriteToPostgresFn()) 
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()