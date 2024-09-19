from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import logging
import os
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'broker:29092').split(',')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'user_log')

# Cassandra Configuration
CASSANDRA_CONTACT_POINTS = os.environ.get('CASSANDRA_CONTACT_POINTS', 'localhost').split(',')
CASSANDRA_PORT = int(os.environ.get('CASSANDRA_PORT', 9042))
CASSANDRA_KEYSPACE = os.environ.get('CASSANDRA_KEYSPACE', 'netflix_keyspace')
CASSANDRA_USERNAME = os.environ.get('CASSANDRA_USERNAME', 'cassandra')
CASSANDRA_PASSWORD = os.environ.get('CASSANDRA_PASSWORD', 'cassandra')

def connect_to_kafka():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def connect_to_cassandra():
    auth_provider = PlainTextAuthProvider(username=CASSANDRA_USERNAME, password=CASSANDRA_PASSWORD)
    cluster = Cluster(CASSANDRA_CONTACT_POINTS, port=CASSANDRA_PORT, auth_provider=auth_provider)
    session = cluster.connect()
    
    # Ensure keyspace exists
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
    """)
    
    # Set the keyspace
    session.set_keyspace(CASSANDRA_KEYSPACE)
    return session


def create_table_if_not_exists(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS user_search_history (
            userId TEXT,
            searchType TEXT,
            keyword TEXT,
            createdAt TIMESTAMP,
            cassandra_timestamp TIMESTAMP,
            PRIMARY KEY ((userId), createdAt, cassandra_timestamp)
        ) WITH CLUSTERING ORDER BY (createdAt DESC, cassandra_timestamp DESC)
    """)

def insert_into_cassandra(session, data):
    query = """
        INSERT INTO user_search_history (userId, searchType, keyword, createdAt, cassandra_timestamp)
        VALUES (%s, %s, %s, %s, toTimestamp(now()))
    """
    session.execute(query, (
        data['userId'],
        data['searchType'],
        data['keyword'],
        datetime.fromisoformat(data['createdAt'])
    ))

def main():
    consumer = connect_to_kafka()
    cassandra_session = connect_to_cassandra()
    create_table_if_not_exists(cassandra_session)

    logging.info("Starting to consume messages from Kafka and insert into Cassandra...")
    
    try:
        for message in consumer:
            data = message.value
            logging.info(f"Received message: {data}")
            insert_into_cassandra(cassandra_session, data)
            logging.info("Inserted data into Cassandra")
    except KeyboardInterrupt:
        logging.info("Stopping consumer due to Keyboard Interrupt...")
    finally:
        consumer.close()
        cassandra_session.shutdown()

if __name__ == "__main__":
    main()