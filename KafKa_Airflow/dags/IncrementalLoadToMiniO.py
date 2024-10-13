from airflow import DAG
from airflow.operators.python import PythonOperator
from cassandra.cluster import Cluster
from io import StringIO
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cassandra_to_minio_incremental',
    default_args=default_args,
    description='A DAG to read new data from Cassandra and upload to MinIO',
    schedule_interval='0 3 * * *',
    start_date=datetime(2024, 9, 18),
    catchup=False,
)

def get_max_timestamp(**kwargs):
    stored_timestamp = Variable.get("latest_processed_timestamp", default_var=None)
    
    if stored_timestamp:
        logging.info(f"Using stored timestamp: {stored_timestamp}")
        return stored_timestamp
    
    logging.info("No stored timestamp found. Searching in MinIO...")
    s3_hook = S3Hook(aws_conn_id="minio_conn")
    bucket_name = 'doanbucket-lqh'

    current_date = datetime.now()
    current_month = current_date.month
    previous_month = (current_date.replace(day=1) - timedelta(days=1)).month 

    files = s3_hook.list_keys(bucket_name, prefix=f'data/t{current_month}')
    if not files:
        files = s3_hook.list_keys(bucket_name, prefix=f'data/t{previous_month}')

    if not files:
        logging.info("No data found in MinIO for the current or previous month. This might be the first run.")
        return None

    latest_file = sorted(files)[-1]

    try:
        obj = s3_hook.get_key(latest_file, bucket_name)
        df = pd.read_csv(StringIO(obj.get()['Body'].read().decode('utf-8')), usecols=['createdat'])
        
        if 'createdat' in df.columns:
            max_timestamp = pd.to_datetime(df['createdat']).max()
            return max_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            logging.warning("Column 'createdat' not found in the CSV file.")
            return None
    except Exception as e:
        logging.error(f"Error reading file from MinIO: {str(e)}")
        return None

def read_from_cassandra_and_upload_to_minio(**kwargs):
    ti = kwargs['ti']
    max_timestamp_str = ti.xcom_pull(task_ids='get_max_timestamp')

    max_timestamp = pd.to_datetime(max_timestamp_str) if max_timestamp_str else None

    cluster = Cluster(['cassandra'], port=9042)
    session = cluster.connect('netflix_keyspace')

    cassandra_max_query = "SELECT MAX(createdat) FROM user_search_history;"
    cassandra_max_timestamp = session.execute(cassandra_max_query).one()[0]

    if max_timestamp and cassandra_max_timestamp <= max_timestamp:
        logging.info('No new data to process.')
        session.shutdown()
        return None

    if max_timestamp:
        query = f"SELECT * FROM user_search_history WHERE createdat > '{max_timestamp}' ALLOW FILTERING;"
    else:
        query = "SELECT * FROM user_search_history;"        

    rows = session.execute(query)
    df = pd.DataFrame(rows)
    session.shutdown()

    if df.empty:
        logging.info("No new data to upload")
        return None
    
    df['createdat'] = pd.to_datetime(df['createdat']).dt.strftime('%Y-%m-%d %H:%M:%S.%f')

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    current_month = datetime.now().month

    s3_hook = S3Hook(aws_conn_id='minio_conn')
    bucket_name = 'doanbucket-lqh'
    file_key = f"data/t{current_month}/{datetime.now().strftime('%Y%m%d')}.csv"

    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=file_key,
        bucket_name=bucket_name,
        replace=True
    )

    logging.info(f"Uploaded new data to MinIO bucket '{bucket_name}' with key '{file_key}'")
    return df['createdat'].max()

def store_latest_timestamp(**kwargs):
    ti = kwargs['ti']
    latest_timestamp = ti.xcom_pull(task_ids='read_from_cassandra_and_upload_to_minio')
    if latest_timestamp:
        Variable.set("latest_processed_timestamp", latest_timestamp)
        logging.info(f"Stored latest processed timestamp: {latest_timestamp}")
    else:
        logging.info("No new timestamp to store")

get_timestamp_task = PythonOperator(
    task_id='get_max_timestamp',
    python_callable=get_max_timestamp,
    dag=dag,
)

read_and_upload_task = PythonOperator(
    task_id='read_from_cassandra_and_upload_to_minio',
    python_callable=read_from_cassandra_and_upload_to_minio,
    dag=dag,
)

store_timestamp_task = PythonOperator(
    task_id='store_latest_timestamp',
    python_callable=store_latest_timestamp,
    dag=dag,
)

get_timestamp_task >> read_and_upload_task >> store_timestamp_task