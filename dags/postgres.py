import pandas as pd
from airflow.operators.python import PythonOperator

import datetime

import airflow
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from faker import Faker
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from pymongo import MongoClient
from variables import data_source_final, sql_source
import os

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

postgres_dag = DAG(
    dag_id='postgres_dag',
    catchup=False,
    default_args=default_args_dict,
    template_searchpath=['/opt/airflow/dags/']
)


def create_meme_table():
    print("Creating meme table")


create_meme_table = PythonOperator(
    task_id='create_meme_table',
    dag=postgres_dag,
    python_callable=create_meme_table,
    trigger_rule='all_success',
)

insert_into_memes = PostgresOperator(
    task_id='insert_into_memes',
    dag=postgres_dag,
    postgres_conn_id='postgres_default',
    sql='sql/memes.sql',
    trigger_rule='all_success',
    autocommit=True
)


def create_authors_table():
    print("Creating meme table")


create_authors_table = PythonOperator(
    task_id='create_authors_table',
    dag=postgres_dag,
    python_callable=create_authors_table,
    trigger_rule='all_success',
)

insert_into_authors = PostgresOperator(
    task_id='insert_into_authors',
    dag=postgres_dag,
    postgres_conn_id='postgres_default',
    sql='sql/authors.sql',
    trigger_rule='all_success',
    autocommit=True
)


def create_updaters_table():
    print("Creating meme table")


create_updaters_table = PythonOperator(
    task_id='create_updaters_table',
    dag=postgres_dag,
    python_callable=create_updaters_table,
    trigger_rule='all_success',
)

insert_into_updaters = PostgresOperator(
    task_id='insert_into_updaters',
    dag=postgres_dag,
    postgres_conn_id='postgres_default',
    sql='sql/updaters.sql',
    trigger_rule='all_success',
    autocommit=True
)


def create_tags_table():
    print("Creating meme table")


create_tags_table = PythonOperator(
    task_id='create_tags_table',
    dag=postgres_dag,
    python_callable=create_tags_table,
    trigger_rule='all_success',
)

insert_into_tags = PostgresOperator(
    task_id='insert_into_tags',
    dag=postgres_dag,
    postgres_conn_id='postgres_default',
    sql='sql/authors.sql',
    trigger_rule='all_success',
    autocommit=True
)

add_relations = PostgresOperator(
    task_id='add_relations',
    dag=postgres_dag,
    postgres_conn_id='postgres_default',
    sql='sql/relations.sql',
    trigger_rule='all_success',
    autocommit=True
)

START = DummyOperator(
    task_id='start_pipeline',
    dag=postgres_dag
)

END = DummyOperator(
    task_id='end_pipeline',
    dag=postgres_dag
)

START >> [create_meme_table, create_authors_table, create_updaters_table, create_tags_table]
create_meme_table >> insert_into_memes
create_authors_table >> insert_into_authors
create_updaters_table >> insert_into_updaters
create_tags_table >> insert_into_tags
[insert_into_memes, insert_into_authors, insert_into_updaters, insert_into_tags] >> add_relations
add_relations >> END
