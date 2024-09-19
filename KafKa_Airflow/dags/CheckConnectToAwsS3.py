from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

def check_s3_connection(bucket_name):
    # Create an S3 hook
    s3_hook = S3Hook(aws_conn_id='aws_default')

    try:
        keys = s3_hook.list_keys(bucket_name)
        if keys:
            print(f"Successfully connected to S3 bucket '{bucket_name}'. Found {len(keys)} objects.")
        else:
            print(f"Successfully connected to S3 bucket '{bucket_name}', but no objects found.")
    except Exception as e:
        print(f"Failed to connect to S3 bucket '{bucket_name}'. Error: {e}")

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    's3_connection_check',
    default_args=default_args,
    description='A DAG to check connection to an S3 bucket',
    schedule_interval=None,  # Set to None for manual trigger
    start_date=days_ago(1),
    catchup=False,
)

# Define the task
with dag:
    check_s3 = PythonOperator(
        task_id='check_s3_connection',
        python_callable=check_s3_connection,
        op_args=['doanbucket-lqh'],  # Replace with your S3 bucket name
    )

    check_s3
