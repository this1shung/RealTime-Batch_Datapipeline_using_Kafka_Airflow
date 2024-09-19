from airflow import DAG
from airflow.operators.python import PythonOperator
from cassandra.cluster import Cluster
from io import StringIO
import pandas as pd
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cassandra_to_s3_incremental',
    default_args=default_args,
    description='A DAG to read new data from Cassandra and upload to S3',
    schedule_interval='0 3 * * *',  # Run at 3 AM every day
    start_date=datetime(2024, 9, 18),
    catchup=False,
)

def get_max_timestamp_from_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'doanbucket-lqh'
    
    latest_file = s3_hook.list_keys(bucket_name, prefix='data/')
    latest_file = sorted(latest_file)[-1] if latest_file else None
    
    if not latest_file:
        return None
    
    obj = s3_hook.get_key(latest_file, bucket_name)
    df = pd.read_csv(StringIO(obj.get()['Body'].read().decode('utf-8')))
    
    if 'createdat' in df.columns:
        max_timestamp = pd.to_datetime(df['createdat']).max()
        # Convert Timestamp to string before returning
        return max_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
    
    return None

def read_from_cassandra_and_upload_to_s3(**kwargs):
    ti = kwargs['ti']
    s3_max_timestamp_str = ti.xcom_pull(task_ids='get_max_timestamp_from_s3')
    
    # Convert string back to datetime if it's not None
    s3_max_timestamp = pd.to_datetime(s3_max_timestamp_str) if s3_max_timestamp_str else None
    
    cluster = Cluster(['cassandra'], port=9042)
    session = cluster.connect('netflix_keyspace')
    
    cassandra_max_query = "SELECT MAX(createdat) FROM user_search_history;"
    cassandra_max_timestamp = session.execute(cassandra_max_query).one()[0]
    
    if s3_max_timestamp and cassandra_max_timestamp <= s3_max_timestamp:
        print("No new data to process.")
        session.shutdown()
        return None
    
    if s3_max_timestamp:
        query = f"SELECT * FROM user_search_history WHERE createdat > '{s3_max_timestamp}' ALLOW FILTERING;"
    else:
        query = "SELECT * FROM user_search_history;"
    
    rows = session.execute(query)
    df = pd.DataFrame(rows)
    session.shutdown()
    
    if df.empty:
        print("No new data to upload.")
        return None
    
    df['createdat'] = pd.to_datetime(df['createdat']).dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    
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
    
    print(f"Uploaded new data to S3 bucket '{bucket_name}' with key '{file_key}'")
    return df['createdat'].max()

def store_latest_timestamp(**kwargs):
    ti = kwargs['ti']
    latest_timestamp = ti.xcom_pull(task_ids='read_from_cassandra_and_upload_to_s3')
    if latest_timestamp:
        Variable.set("latest_processed_timestamp", latest_timestamp)

get_s3_timestamp_task = PythonOperator(
    task_id='get_max_timestamp_from_s3',
    python_callable=get_max_timestamp_from_s3,
    dag=dag,
)

read_and_upload_task = PythonOperator(
    task_id='read_from_cassandra_and_upload_to_s3',
    python_callable=read_from_cassandra_and_upload_to_s3,
    dag=dag,
)

store_timestamp_task = PythonOperator(
    task_id='store_latest_timestamp',
    python_callable=store_latest_timestamp,
    dag=dag,
)

get_s3_timestamp_task >> read_and_upload_task >> store_timestamp_task