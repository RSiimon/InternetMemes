import datetime

import airflow
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from faker import Faker
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import os

import warnings

from cleaning1 import cleaning_1
from cleaning2 import cleaning_2
from cleaning3 import cleaning_3

warnings.filterwarnings('ignore')

fake = Faker()
log_tag = "[INGESTION] "
target_1 = "kym.json"
target_2 = "kym_spotlight.json"
target_3 = "kym_vision.json"
base_url = 'https://owncloud.ut.ee/owncloud/index.php/s/g4qB5DZrFEz2XLm/download/'
client = MongoClient('mongodb://mongo:27017')  # Docker client
db = client["memes"]
collection_1 = db["kym"]
collection_2 = db["kym_spotlight"]
collection_3 = db["kym_vision"]

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

cleaning_dag = DAG(
    dag_id='cleaning_dag',
    catchup=False,
    default_args=default_args_dict,
    template_searchpath=['/opt/airflow/dags/']
)

clean_first = PythonOperator(
    task_id='clean_first',
    dag=cleaning_dag,
    python_callable=cleaning_1,
    trigger_rule='all_success',
    depends_on_past=False,
)

clean_second = PythonOperator(
    task_id='clean_second',
    dag=cleaning_dag,
    python_callable=cleaning_2,
    trigger_rule='all_success',
    depends_on_past=False,
)

clean_third = PythonOperator(
    task_id='clean_third',
    dag=cleaning_dag,
    python_callable=cleaning_3,
    trigger_rule='all_success',
    depends_on_past=False,
)

START = BashOperator(task_id='create_dir',
                  bash_command="cd /opt/airflow/dags/data/ ; mkdir -p cleaned_data ; mkdir -p json_data ; mkdir tmp", dag=cleaning_dag)

COMPLETE = DummyOperator(
    task_id='end_pipeline',
    dag=cleaning_dag
)

START >> clean_first >> clean_second >> clean_third >> COMPLETE