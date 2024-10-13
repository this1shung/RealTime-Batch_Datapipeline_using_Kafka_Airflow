from airflow import DAG
from airflow.operators.python import PythonOperator
from cassandra.cluster import Cluster
from io import StringIO
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cassandra_to_minio_full_load',
    default_args=default_args,
    description='A DAG to read all data from Cassandra and upload to MinIO',
    schedule_interval='0 3 * * *',
    start_date=datetime(2024, 9, 18),
    catchup=False,
)

def read_from_cassandra_and_upload_to_minio(**kwargs):
    cluster = Cluster(['cassandra'], port=9042)
    session = cluster.connect('netflix_keyspace')

    query = "SELECT * FROM user_search_history;"
    rows = session.execute(query)
    df = pd.DataFrame(rows)
    session.shutdown()

    if df.empty:
        logging.info("No data to upload")
        return None
    
    df['createdat'] = pd.to_datetime(df['createdat']).dt.strftime('%Y-%m-%d %H:%M:%S.%f')

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    current_date = datetime.now().strftime('%Y%m%d')
    current_month = current_date.month

    s3_hook = S3Hook(aws_conn_id='minio_conn')
    bucket_name = 'doanbucket-lqh'
    file_key = f"data/t{current_month}/{current_date}_full_load.csv"

    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=file_key,
        bucket_name=bucket_name,
        replace=True
    )

    logging.info(f"Uploaded full data to MinIO bucket '{bucket_name}' with key '{file_key}'")
    return len(df)

read_and_upload_task = PythonOperator(
    task_id='read_from_cassandra_and_upload_to_minio',
    python_callable=read_from_cassandra_and_upload_to_minio,
    dag=dag,
)

read_and_upload_task