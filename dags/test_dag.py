import datetime

import airflow
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from faker import Faker
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook

import os
from neo4j import GraphDatabase, basic_auth

from pymongo import MongoClient
import os
import time
import warnings

import pandas as pd 

warnings.filterwarnings('ignore')

NEO4J_USER = "neo4j"
NEO4J_URI = "bolt://172.17.0.2:7687" #172.17.0.2 docker container ip - to check cmd: docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' container_name_or_id
NEO4J_PASSWORD = "internet" # if ran for the first time maybe pwd is neo4j instead


default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

testing = DAG(
    dag_id='testing',
    catchup=False,
    default_args=default_args_dict,
    dagrun_timeout=datetime.timedelta(seconds=4200),
    template_searchpath=['/opt/airflow/dags/']
)

def _connect_first():
    db_hook = Neo4jHook()
    db_conn = db_hook.get_conn()

    
    with db_conn.session() as session:

        cypher_query = "Match () Return 1 Limit 1"
        result = session.run(cypher_query)
        print("ok")
    #graph = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


connect_first = PythonOperator(
    dag=testing,
    task_id='connect_first',
    python_callable=_connect_first,
    trigger_rule='all_success',
    depends_on_past=False
)

START = DummyOperator(
    dag=testing,
    task_id='start_pipeline'
)

START >> connect_first