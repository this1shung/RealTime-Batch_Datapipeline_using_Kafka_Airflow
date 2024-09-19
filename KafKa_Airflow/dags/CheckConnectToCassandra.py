from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from cassandra.cluster import Cluster
from datetime import datetime

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 18),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'cassandra_read_dag',
    default_args=default_args,
    description='A DAG to read from Cassandra',
    schedule_interval='@daily',
)

def read_from_cassandra(**kwargs):
    # Connect to Cassandra
    cluster = Cluster(['cassandra'], port=9042)  # Cassandra container IP and port
    session = cluster.connect()

    # Example query - replace with your actual query
    query = "SELECT * FROM netflix_keyspace.user_search_history;"
    rows = session.execute(query)

    for row in rows:
        print(row)

    session.shutdown()

# Define tasks
read_task = PythonOperator(
    task_id='read_from_cassandra',
    python_callable=read_from_cassandra,
    dag=dag,
)

# Set task dependencies
read_task