from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

current_month = datetime.now().month

dag = DAG(
    'Process_Data',
    default_args=default_args,
    description='A DAG to test Spark application that shows data using Docker',
    schedule_interval='@once',  
    start_date=datetime(2024, 9, 28),
    catchup=False,
)

with dag:
    show_data = DockerOperator(
    task_id='etl_data',
    max_active_tis_per_dag=1,
    image='airflow/spark-app',
    container_name='etl_data_job',
    api_version='auto',
    auto_remove=True,
    docker_url='tcp://docker-proxy:2375',
    network_mode='container:spark-master',
    tty=True,
    xcom_all=False,
    mount_tmp_dir=False,
    dag=dag  
    )

    show_data 


