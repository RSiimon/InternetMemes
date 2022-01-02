from neo4j import GraphDatabase
from py2neo import Graph 
import pandas as pd 
import numpy as np

NEO4J_USER = "neo4j"
NEO4J_URI = "bolt://172.17.0.2:7687"
NEO4J_PASSWORD = "internet"

### IF AIRFLOW NOT WORKING, USE THIS SAVE FINAL FILE TO AIRFLOW import folder and Load from there or SETUP the localhost situation like in graph_db.ipynb
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

with neo4j_driver.session() as session:


    try:
        cypher_query = "Match () Return 1 Limit 1"
        result = session.run(cypher_query)
        print("ok")
        query_main = "LOAD CSV WITH HEADERS FROM file:///main.csv AS csvLine" \
        "LOAD CSV WITH HEADERS FROM file:///relations.csv AS csvL" \
        "MERGE (m:Meme {meme_name:csvLine.meme_name, url:csvLine.url, views:toInteger(csvLine.views)})" \
        "MERGE (p:Meme_parent {parent_name: csvL.parent})" \
        "MERGE (c:Meme_children {kids: csvL.children})" \
        "MERGE (s:Meme_siblings {siblings: csvL.siblings})" \
        "MERGE (a:Meme_author {name: csvLine.author})" \
        "MERGE (r:Recent_update {updated: csvLine.updated})" \
        "MERGE (t:Meme_template {template_image: csvLine.template_image_url})" \
        "CREATE (m)-[:BELONGS_TO]->(p), (m)-[:IS_RELATED_TO]->(s), (m)-[:IS_PARENT_TO]->(c)" \
        "CREATE (a)-[:CREATED]->(m), (m)-[:BASED_ON]->(t), (m)-[:RECENTLY_UPDATED_ON]->(r)"

        result = session.run(query_main)

    except:
        print("not ok")





#base_url = "https://drive.google.com/file/d/16vcRWIoZQhavcx8i_C7RaXWvkWLiKxmg/view?usp=sharing"
# reading it in from cleaned data
#url='https://drive.google.com/uc?id=' + base_url.split('/')[-2]
#main_df = pd.read_csv("/home/marilin/Documents/Data engineering/data_project/main.csv", encoding='utf-8', index_col=False) 
#print(main_df.head())
#del main_df["description"]
#main_df = main_df.dropna()
# saving to local neo4j folder 
#main_df.to_csv("/home/marilin/.config/Neo4j Desktop/Application/relate-data/dbmss/dbms-fc4db720-e397-4b7a-a229-692d78772748/import/main.csv", encoding='utf-8', index=False)


