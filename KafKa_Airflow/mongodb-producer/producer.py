from pymongo import MongoClient
import pandas as pd
from kafka import KafkaProducer
import json
from bson import ObjectId
import logging
from datetime import datetime
import time
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

def get_data_from_change_stream():
    MONGO_URI = os.environ.get('MONGODB_URI', "mongodb+srv://hungqlworking:IC96WUPzMTJzCkI1@cluster0.temagzq.mongodb.net/netflix_DB?retryWrites=true&w=majority&appName=Cluster0")
    client = MongoClient(MONGO_URI)
    db = client["netflix_DB"]
    collection = db["searchhistories"]

    logging.info("Starting to monitor MongoDB for changes...")
    with collection.watch() as stream:
        for change in stream:
            if change['operationType'] in ['insert', 'update', 'delete']:
                document = change.get('fullDocument')
                if document:
                    logging.info(f"Detected change in MongoDB: {document}")
                    yield document
            else:
                logging.info("Change detected but no document to process.")
                yield None

def format_data(data):
    df = pd.DataFrame([data]) 
    if not df.empty:
        new_df = df[['userId', 'searchType', 'keyword', 'createdAt']]  
        return new_df.to_dict(orient='records')
    return None

def stream_data():
    kafka_bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'broker:29092').split(',')
    kafka_topic = os.environ.get('KAFKA_TOPIC', 'user_log')

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        max_block_ms=5000,
        value_serializer=lambda v: JSONEncoder().encode(v).encode('utf-8')
    )

    try:
        for document in get_data_from_change_stream():
            if document:
                formatted_data = format_data(document)
                if formatted_data:
                    logging.info(f"Sending formatted data to Kafka: {formatted_data}")
                    for record in formatted_data:
                        producer.send(kafka_topic, value=record)
                    producer.flush()
            else:
                logging.info("No new data to send.")
            time.sleep(2) 
    except KeyboardInterrupt:
        logging.info("Stopping Kafka producer due to Keyboard Interrupt...")
    finally:
        producer.close()

if __name__ == "__main__":
    stream_data()