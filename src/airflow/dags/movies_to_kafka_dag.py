from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

MOVIES_FILE_PATH = os.getenv("MOVIES_FILE_PATH")
PROCESSED_INPUT_PATH = os.getenv("PROCESSED_INPUT_PATH")
MOVIE_PRODUCER_PATH : str = os.getenv("MOVIE_PRODUCER_PATH")
MOVIE_PROCESSOR_PATH : str = os.getenv("MOVIE_PROCESSOR_PATH")
PYTHONPATH : str = os.getenv("PYTHONPATH")

default_args = {
    "owner": "data_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="movie_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    file_sensor = FileSensor(
        task_id="detect_new_movie_file",
        filepath=MOVIES_FILE_PATH,
        poke_interval=300,
        timeout=3600
    )

    process_task = BashOperator(
        task_id="process_movie_data",
        bash_command= f'PYTHONPATH={PYTHONPATH} && '
                f'spark-submit {MOVIE_PROCESSOR_PATH}'
    )

    produce_task = BashOperator(
        task_id="produce_kafka_events",
        bash_command= f'PYTHONPATH={PYTHONPATH} && '
                f'spark-submit {MOVIE_PRODUCER_PATH}'
    )

    cleanup_task = BashOperator(
        task_id="cleanup_processed_data",
        bash_command=f"hdfs dfs -rm -r {PROCESSED_INPUT_PATH}*"
    )

    file_sensor >> process_task >> produce_task >> cleanup_task
