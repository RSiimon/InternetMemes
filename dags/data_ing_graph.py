import datetime

import airflow
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from faker import Faker
from airflow import DAG
from airflow.operators.python import PythonOperator

import os
from neo4j import GraphDatabase, basic_auth

from pymongo import MongoClient
import os
import time
import warnings

from cleaning1 import cleaning_1
from cleaning2 import cleaning_2
from cleaning3 import cleaning_3
from add_authors import add_authors
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

graph_model_dag = DAG(
    dag_id='graph_model_dag',
    catchup=False,
    default_args=default_args_dict,
    dagrun_timeout=datetime.timedelta(seconds=4200),
    template_searchpath=['/opt/airflow/dags/']
)

def _connect_first():
    graph = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


    ## checking if the server is up and running 
    with graph.session(database="neo4j") as session:

        cypher_query = "Match () Return 1 Limit 1"
        result = session.run(cypher_query)
        print("ok")




connect_first = PythonOperator(
    task_id='connect_first',
    dag=graph_model_dag,
    python_callable=_connect_first,
    trigger_rule='all_success',
    depends_on_past=False
)

def _main_changes():


    graph = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD), connection_timeout=10000000000000, encrypted = False)
    # main_csv should be in final_data folder
    main_df = pd.read_csv("/opt/airflow/dags/data/final_data/main.csv", encoding='utf-8', index_col=False)

    main_df = main_df.dropna()
    del main_df["description"]
    # creating a column with similar tags as relations_df to get a node connection
    lst = []
    for link in list(main_df["url"]):
        lst.append(link.split("/")[-1])

    main_df["meme_name"] = lst
    main_df.to_csv("/opt/airflow/dags/data/import/main.csv", encoding='utf-8', index=False) 



    with graph.session(database="neo4j") as session:
        query_main = "LOAD CSV WITH HEADERS FROM file:///main.csv AS csvLine" \
        "MERGE (m:Meme_name {meme_name:csvLine.meme_name, url:csvLine.url, views:toInteger(csvLine.views)})" \
        "MERGE (a:Meme_author {name: csvLine.author})" \
        "MERGE (r:Recent_update {updated: csvLine.updated})" \
        "MERGE (t:Meme_template {template_image: csvLine.template_image_url})" \
        "CREATE (m)-[:IS_CREATED_BY]->(a), (m)-[:BASED_ON]->(t), (m)-[:RECENTLY_UPDATED_ON]->(r)"
        
        result = session.run(query_main)
        print("ok")
   

main_changes = PythonOperator(
    task_id='main_changes',
    dag=graph_model_dag,
    python_callable=_main_changes,
    trigger_rule='all_success',
    depends_on_past=False,
    execution_timeout=datetime.timedelta(seconds=4200)
)

def _relations_changes():
    graph = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD), connection_timeout=10000000000000, encrypted = False)

    # relations.csv should be in final_data folder
    relations_df = pd.read_csv("/opt/airflow/dags/data/final_data/relations.csv", encoding='utf-8')
    relations_df = relations_df.rename(columns={'Unnamed: 0': 'meme_name'})
    relations_df = relations_df.fillna("unknown")

    del relations_df["children"]

    memes = relations_df["meme_name"].tolist()
    siblings = relations_df["siblings"].tolist()
    parents = relations_df["parent"].tolist()

    meme_col, sib_col, p_col = [], [], []

    for i, meme in enumerate(memes):

        meme_labels = siblings[i].strip("[]'").split("', '")


        for j in range(min(5, len(meme_labels))):
            p_col.append(parents[i])
            meme_col.append(meme)
            sib_col.append(meme_labels[j])


    mfa = pd.DataFrame()
    mfa["meme_name"] = meme_col
    mfa["parent"] = p_col
    mfa["siblings"] = sib_col

    mfa.to_csv("/opt/airflow/dags/data/import/relations.csv", encoding='utf-8', index=False) 
    
    with graph.session(database="neo4j") as session:

        query_main = "LOAD CSV WITH HEADERS FROM file:///relations.csv AS csvLine" \
        "MERGE (p:Meme_parent {parent_name: csvLine.parent})" \
        "MERGE (s:Meme_siblings {siblings: csvLine.siblings})" \
        "MERGE (m:Meme_name {meme_name:csvLine.meme_name})" \
        "CREATE (m)-[:IS_CHILD_OF]->(p), (m)-[:IS_RELATED_TO]->(s)"

        result = session.run(query_main)

relations_changes = PythonOperator(
    task_id='relations_changes',
    dag=graph_model_dag,
    python_callable=_relations_changes,
    trigger_rule='all_success',
    depends_on_past=False,
    execution_timeout=datetime.timedelta(seconds=4200)
)

def _img_changes():
    graph = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD), connection_timeout=10000000000000, encrypted = False)

    # img_rec.csv should be in final_data folder
    imgrec_df= pd.read_csv("/opt/airflow/dags/data/final_data/img_rec.csv", encoding='utf-8')
    imgrec_df = imgrec_df.rename(columns={'Unnamed: 0': 'meme_name'})
    imgrec_df = imgrec_df.fillna("unknown")

    memes = imgrec_df["meme_name"].tolist()
    labels = imgrec_df['image_recogn_labels'].tolist()

    meme_col, label_col = [], []

    for i, meme in enumerate(memes):
        meme_labels = labels[i].strip("[]'").split("', '")

        for j in range(min(5, len(meme_labels))):
            meme_col.append(meme)
            label_col.append(meme_labels[j])


    mf = pd.DataFrame()
    mf["meme_name"] = meme_col
    mf["image_recogn_labels"] = label_col

    mf.to_csv("/opt/airflow/dags/data/import/img_rec.csv", encoding='utf-8', index=False)
    
    with graph.session(database="neo4j") as session:
        query_main = "LOAD CSV WITH HEADERS FROM file:///img_rec.csv AS csvLine" \
        "MERGE (m:Meme_name {meme_name:csvLine.meme_name})" \
        "MERGE (i: image_recogn_labels{image_label_name:csvLine.image_recogn_labels})" \
        "CREATE (m)-[:CONTAINS_]->(i)"

        result = session.run(query_main)


img_changes = PythonOperator(
    task_id='img_changes',
    dag=graph_model_dag,
    python_callable=_img_changes,
    trigger_rule='all_success',
    depends_on_past=False,
    execution_timeout=datetime.timedelta(seconds=4200)
)    

START = BashOperator(task_id='create_import_dir',
                  bash_command="cd /opt/airflow/dags/data ; mkdir -p import", dag=graph_model_dag)

COMPLETE = DummyOperator(
    task_id='end_pipeline',
    dag=graph_model_dag
)

START >> connect_first 
connect_first >> main_changes
main_changes >> relations_changes
relations_changes >> img_changes
img_changes >> COMPLETE