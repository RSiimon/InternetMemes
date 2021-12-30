import datetime

import airflow
import requests
from airflow.operators.dummy import DummyOperator
from faker import Faker
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import json

import warnings

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

ingestion_dag = DAG(
    dag_id='ingestion_dag',
    catchup=False,
    default_args=default_args_dict,
    template_searchpath=['/opt/airflow/dags/']
)


def download_file(target):
    """
    Checks if the file is present and depending on that marks as done or downloads it
    """
    print(log_tag + "Started the download file function with target:", target)

    file_valid = True
    if os.path.isfile(target):
        print(log_tag + "File already exists, skipping this part")
        file_valid = False
    with open(target, 'w+') as f:
        if len(f.readlines()) == 0:
            file_valid = False
            print(log_tag + "File empty, overwriting")
    if not file_valid:
        print(log_tag + "Downloading the file")
        r = requests.get(base_url + target, allow_redirects=True)
        print(log_tag + "Got response from URL:", r.status_code)
        print(log_tag + "Check the data pulled", r.json()[0])
        with open(target, 'w+', encoding='utf-8') as f:
            json.dump(r.json(), f, ensure_ascii=False)
            print(log_tag + "Dumped json into the file")


def write_to_mongo():
    """
    Inserts downloaded file into mongoDB
    """

    with open(target_1, 'r+', encoding='utf-8') as f:
        data = json.load(f)
        collection_1.insert_many(data)
        print(log_tag, "Test if the data is present:", db.kym.find_one())

    with open(target_2, 'r+', encoding='utf-8') as f:
        data = json.load(f)
        collection_2.insert_many(data)
        print(log_tag, "Test if the data is present:", db.kym_spotlight.find_one())

    with open(target_3, 'r+', encoding='utf-8') as f:
        data = json.load(f)
        collection_2.insert_many(data)
        print(log_tag, "Test if the data is present:", db.kym_vision.find_one())


read_first = PythonOperator(
    task_id='download_file_1',
    dag=ingestion_dag,
    python_callable=download_file,
    op_kwargs={
        "target": target_1
    },
    trigger_rule='all_success',
    depends_on_past=False,
)
read_second = PythonOperator(
    task_id='download_file_2',
    dag=ingestion_dag,
    python_callable=download_file,
    op_kwargs={
        "target": target_2
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

read_third = PythonOperator(
    task_id='download_file_3',
    dag=ingestion_dag,
    python_callable=download_file,
    op_kwargs={
        "target": target_3
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

write_mongo = PythonOperator(
    task_id='write_into_mongo',
    dag=ingestion_dag,
    python_callable=write_to_mongo,
    trigger_rule='all_success',
    depends_on_past=False,
)
START = DummyOperator(
    task_id='start_pipeline',
    dag=ingestion_dag
)
COMPLETE = DummyOperator(
    task_id='end_pipeline',
    dag=ingestion_dag
)

START >> [read_first, read_second, read_third]
[read_first, read_second, read_third] >> write_mongo
write_mongo >> COMPLETE
