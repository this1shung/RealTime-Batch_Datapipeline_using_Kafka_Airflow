from datetime import datetime, timedelta
import pandas as pd
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

current_month = datetime.now().month
bucket_name = 'doanbucket-lqh'
prefix = f'processed/t{current_month}/data.csv/'

def list_s3_files(bucket, prefix):
    """Function to list the first CSV file in S3."""
    connection = BaseHook.get_connection('minio_conn')
    s3_client = boto3.client(
        's3',
        endpoint_url='http://host.docker.internal:9000',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
    )
    
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.csv'):
                return obj['Key']

    return None

def load_csv_to_postgres(**kwargs):
    ti = kwargs['ti']
    s3_key = ti.xcom_pull(task_ids='list_files_task')
    
    if not s3_key:
        raise ValueError("No CSV file found in S3")

    # Connect to S3/MinIO
    connection = BaseHook.get_connection('minio_conn')
    s3_client = boto3.client(
        's3',
        endpoint_url='http://host.docker.internal:9000',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
    )

    # Read CSV file from S3/MinIO
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    df = pd.read_csv(response['Body'])

    # Connect to Postgres
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    engine = pg_hook.get_sqlalchemy_engine()

    # Get the current number of rows
    with engine.connect() as connection:
        result = connection.execute("SELECT COUNT(*) FROM doan_db.\"log_search_data\"")
        rows_before = result.fetchone()[0]

    # Append data to Postgres
    df.to_sql('log_search_data', engine, schema='doan_db', if_exists='append', index=False)

    # Get the new number of rows
    with engine.connect() as connection:
        result = connection.execute("SELECT COUNT(*) FROM doan_db.\"log_search_data\"")
        rows_after = result.fetchone()[0]

    print(f"Rows before: {rows_before}, Rows after: {rows_after}, Rows added: {rows_after - rows_before}")

dag = DAG(
    'ImportDataToPostgres',
    default_args=default_args,
    description='Import data to Postgres',
    schedule_interval='@once',
    start_date=datetime(2024, 9, 28),
    catchup=False,
)

with dag:
    list_files_task = PythonOperator(
        task_id='list_files_task',
        python_callable=list_s3_files,
        op_args=[bucket_name, prefix],
        provide_context=True,
        do_xcom_push=True
    )

    load_files_task = PythonOperator(
        task_id='load_files_task',
        python_callable=load_csv_to_postgres,
        provide_context=True
    )

    list_files_task >> load_files_task