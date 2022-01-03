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


def create_meme_query(file, sql_file):
    df = pd.read_csv(file)
    with open(sql_file, "w") as f:
        df_iterable = df.iterrows()
        f.write(
            "CREATE TABLE IF NOT EXISTS intavolature (\n"
            "title VARCHAR(255),\n"
            "subtitle VARCHAR(255),\n"
            "key VARCHAR(255),\n"
            "difficulty VARCHAR(255),\n"
            "date VARCHAR(255),\n"
            "ensemble VARCHAR(255));\n"
        )
        for index, row in df_iterable:
            piece = row['Piece']
            type = row['Type']
            key = row['Key']
            difficulty = row['Difficulty']
            date = row['Date']
            ensemble = row['Ensemble']

            f.write(
                "INSERT INTO intavolature VALUES ("
                f"'{piece}', '{type}', '{key}', '{difficulty}', '{date}', '{ensemble}'"
                ");\n"
            )

        f.close()


task_six_a = PythonOperator(
    task_id='create_intavolature_query',
    dag=postgres_dag,
    python_callable=create_meme_query,
    op_kwargs={
        'file':  data_source_final + 'main_df.csv',
        'sql_file': sql_source + 'meme_insert.sql'
    },
    trigger_rule='all_success',
)


def create_memes_query(file):
    df = pd.read_csv(file)
    with open("/opt/airflow/dags/sql/composer_inserts.sql", "w") as f:
        df_iterable = df.iterrows()
        f.write(
            "CREATE TABLE IF NOT EXISTS composer (\n"
            "name VARCHAR(255)\n"
            ");\n"
        )
        for index, row in df_iterable:
            composer = row['Composer']

            f.write(
                "INSERT INTO composer VALUES ("
                f"'{composer}'"
                ");\n"
            )


task_six_b = PythonOperator(
    task_id='create_composer_query',
    dag=postgres_dag,
    python_callable=create_memes_query,
    op_kwargs={
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success',
)

task_seven_a = PostgresOperator(
    task_id='insert_intavolature_query',
    dag=postgres_dag,
    postgres_conn_id='postgres_default',
    sql='intavolature_inserts.sql',
    trigger_rule='all_success',
    autocommit=True
)
