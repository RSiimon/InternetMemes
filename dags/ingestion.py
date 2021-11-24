import datetime

import airflow
import requests
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
target = "kym.json"
base_url = 'https://owncloud.ut.ee/owncloud/index.php/s/g4qB5DZrFEz2XLm/download/kym.json'
client = MongoClient('mongodb://mongo:27017')  # Docker client
db = client["memes"]
collection = db["data"]

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


def download_file():
    """
    Checks if the file is present and depending on that marks as done or downloads it
    """
    print(log_tag + "Started the download file function")

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
        r = requests.get(base_url, allow_redirects=True)
        print(log_tag + "Download finished, writing into the file")
        with open(target, 'w+', encoding='utf-8') as f:
            json.dump(r.json(), f, ensure_ascii=False)


def write_to_mongo():
    """
    Inserts downloaded file into mongoDB
    """

    with open(target, 'r+', encoding='utf-8') as f:
        data = json.load(f)
        collection.insert_many(data)
        print(log_tag, "Test if the data is present:", db.data.find_one())

task_one = PythonOperator(
    task_id='download_file',
    dag=ingestion_dag,
    python_callable=download_file,
    trigger_rule='all_success',
    depends_on_past=False,
)

task_two = PythonOperator(
    task_id='write_into_mongo',
    dag=ingestion_dag,
    python_callable=write_to_mongo,
    trigger_rule='all_success',
    depends_on_past=False,
)

task_one >> task_two
