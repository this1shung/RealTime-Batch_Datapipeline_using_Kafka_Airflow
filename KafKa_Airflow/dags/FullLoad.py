from airflow import DAG
from airflow.operators.python import PythonOperator
from cassandra.cluster import Cluster
from io import StringIO
import pandas as pd
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'cassandra_to_s3',
    default_args=default_args,
    description='A DAG to read from Cassandra, convert to CSV, and upload to S3',
    schedule_interval='@daily',
    start_date=datetime(2024, 9, 18),
    catchup=False,
)

def read_from_cassandra_and_upload_to_s3(**kwargs):

    cluster = Cluster(['cassandra'], port=9042)  
    session = cluster.connect('netflix_keyspace') 

    query = "SELECT * FROM user_search_history;"
    rows = session.execute(query)

    df = pd.DataFrame(rows)
    session.shutdown()

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

   
    s3_hook = S3Hook(aws_conn_id='aws_default')  
    bucket_name = 'doanbucket-lqh'  
    file_key = f"data/{datetime.now().strftime('%Y%m%d')}.csv"
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=file_key,
        bucket_name=bucket_name,
        replace=True
    )

    print(f"Uploaded file to S3 bucket '{bucket_name}' with key '{file_key}'")

read_and_upload_task = PythonOperator(
    task_id='read_from_cassandra_and_upload_to_s3',
    python_callable=read_from_cassandra_and_upload_to_s3,
    dag=dag,
)

read_and_upload_task
