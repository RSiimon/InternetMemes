# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import datetime
from datetime import date
import json 

fn_authors = "web_scraping.txt"
fname_main_df = 'main_df.csv'
fname2_main_df = 'main_df.csv'

path_to_cleaned = "cleaned_data//"
path_to_cleaned_tmp = "cleaned_data//tmp_with_lists//"


# ---------------------------------------------

# LOAD FILES (ALSO MAIN TABLE IN BOTH CLEANED FOLDERS):

# Convert columns containing lists from str to list:
def convert_list_col(col, df2):
    col_list = df2[col].tolist()
    col2 = []
    for row in col_list:
        if isinstance(row, str):
            col2.append(row.strip("[]'").split("', '")) 
        else:
            col2.append(np.nan)
    df2[col] = col2
    return df2

# Convert dates to date format:
def convert_date_col(date_col, df2):
    dates = df2[date_col]
    dates2 = []
    for el in dates:
        if isinstance(el, str) and len(el)!=0:
            dates2.append(datetime.datetime.strptime(el, '%Y-%m-%d').date())
        else:
            dates2.append(np.nan)
    df2[date_col] = dates2
    return df2
    

def load_main(fname, folder, has_lists):
    df = pd.read_csv(folder + fname, sep=';', index_col=0, encoding='utf-8')    
    if has_lists:
        df = convert_list_col('similar_images', df) 
        df = convert_list_col('similar_images2', df) 
        df = convert_list_col('image_recogn_labels', df) 
        df = convert_list_col('image_recogn_scores', df) 
        df = convert_list_col('alt_img_urls', df) 
        df = convert_list_col('search_keywords', df) 
        
    df['year'] = df['year'].astype(pd.Int32Dtype())  
    df = convert_date_col('added', df)
    df = convert_date_col('updated', df)
    
    return df


def load_authors(filename):
    with open(filename,'r') as f: 
        lines = f.readlines()  
        
    # Make df:
    lines2 = [row.strip().split(',') for row in lines]
    df = pd.DataFrame(data =lines2) 
    df.columns = ['row_no', 'url', 'code', 'views', 'author', 'updater', 'added', 'updated']
    
    # Convert integers from str to int:
    for col in ['row_no', 'code', 'views']:
        df[col] = df[col].astype('int32')
    
    # Convert dates to date format:
    df = convert_date_col('added', df)
    df = convert_date_col('updated', df) 
    df['ids'] = pd.Series([x.split('/')[-1] for x in df['url'].tolist()])
    
    return df


df_main = load_main(fname_main_df, path_to_cleaned, False)
df_main2 = load_main(fname2_main_df, path_to_cleaned_tmp, True)
df = load_authors(fn_authors)

# ---------------------------------------------

# LEAVE OUT ROWS NOT PRESENT IN MAIN TABLE:

# - also: add ids as index

ids_main = df_main.index.tolist()  
ids_authors =  df['ids'].tolist()
dropped = list(set(ids_authors).difference(set(ids_main)))
df = df[df['ids'].isin(dropped) == False]  
# len(df)==len(df_main)  #True
# len(list(set(df['ids'].tolist()))) == len(df_main) # True
# len(list(set(df['ids'].tolist()))) == len(df)  # True -> no duplicates

df.index = df['ids'].tolist()

# ---------------------------------------------

# LEAVE OUT AUTHORS AND UPDATERS WITH NON-ASCII OR NUMERIC NAMES:

def exclude_persons(df, col, excluded):
    persons = df[col].tolist()
    persons2 = []
    for el in persons:
        if el not in excluded:
            persons2.append(el)
        else:
            persons2.append(np.nan)
    df[col] = persons2
    return df

a = sorted(df['author'].tolist())
u = sorted(df['updater'].tolist())
exclude_auth = a[:23] + ["", "200"]
exclude_updater = [u[133], ""]

df = exclude_persons(df, 'author', exclude_auth)
df = exclude_persons(df, 'updater', exclude_auth)          
    
# ---------------------------------------------

# SOME CHECKS:

    
# 1) Check if sever always responded with response code 200:
    
# faulty_rows = df[df['code']!=200]['row_no'].tolist() 
# faulty = df[df['row_no'].isin(faulty_rows)] 

# a = df_main[df_main.index.isin(faulty.index.tolist())]   # 6
# # -> 5 have  status = "deadpool", 1 has status = "submission".

# faulty
# [2043, 6433, 7631, 9445, 9805, 11087]  -> checked: pages of those memes no longer exist 

# -> Dropped those 6 memes in cleaning1.py. (Otherwise we would need to load  all tables and jsons here, only just to leave out those 6 memes, and then save them again. No need to repeat the code - can be done earlier.)


# ---------------------------------------------

#  "ADDED" AND "UPDATED" DATES:

# Compare dates in main table with dates from web scraping:
# df = df.drop(columns = ['author', 'updater', 'views'])
df['added2'] = df_main['added']
df['updated2'] = df_main['updated']
# df = df[['added', 'added2', 'updated', 'updated2']]
# df['added_same'] = df['added'] == df['added2']
# df['updated_same'] = df['updated'] == df['updated2']

added = df['added'].tolist()
added2 =  df['added2'].tolist()
updated = df['updated'].tolist()
updated2 =df['updated2'].tolist()
added3, updated3 = [], []

for i in range(len(df)):
    a = added[i]
    a2 = added2[i]
    u = updated[i]
    u2 = updated2[i]
    
    # Added dates - use earliest date:
    if isinstance(a, float): 
        added3.append(a2)
    elif isinstance(a2, float) or a<a2:
        added3.append(a)
    else:
        added3.append(a2)
        
    # Last update dates - use latest date:
    if isinstance(u, float): 
        updated3.append(u2)
    elif isinstance(u2, float) or u>u2: 
        updated3.append(u)
    else:
        updated3.append(u2)
        
df['added3'] = added3
df['updated3'] = updated3

# ---------------------------------------------

# INFERRING MISSING VALUES FOR "YEAR" AND "ADDED" COLUMN:
    
# -in 144 cases where “added” was missing, we infer the date by using “updated” date as “added”,
# - in 991 cases where “year” was missing, we infer it from “added”. 

df['year'] = df_main['year']
years = df['year'].tolist()

for i, el in enumerate(updated3):
    added_date = added3[i]
    if isinstance(added_date, float):
        added3[i]=el  
        
for i, el in enumerate(added3):
    year = years[i]
    if isinstance(year, np.int32)==False:     
        years[i]=el.year
        
df['added3'] = added3
df['year'] = years

# ---------------------------------------------
  
# ADD COLUMNS TO MAIN TABLE:
# - ADD: AUTHORS, LAST UPDATERS AND NO. OF VIEWS 
# - UPDATE: ADDED, UPDATED DATES
    
# df = df.drop(columns = ['row_no', 'url', 'code', 'ids'])

def add_cols_to_main(df2, df):
    df2['author'] = df['author']
    df2['updater'] = df['updater']
    df2['views'] = df['views']
    df2['added'] = df['added3']
    df2['updated'] = df['updated3']   
    df2['year'] = df['year'] 
    return df2

df_main = add_cols_to_main(df_main, df)
df_main2 = add_cols_to_main(df_main2, df)

# Reorder columns:
df_main = df_main[['title', 'url', 'description', 'views', 'author', 'updater', 'added', 'updated', 'year', 'has_relations', 'has_other_texts', 'has_refs', 'has_DBpedia', 'status', 'adult', 'medical', 'racy', 'spoof', 'violence', 'template_image_url', 'fb_image_url']]

df_main2 = df_main2[['title', 'url', 'description', 'views', 'author', 'updater', 'added', 'updated', 'year', 'has_relations', 'has_other_texts', 'has_refs', 'has_DBpedia', 'status', 'adult', 'medical', 'racy', 'spoof', 'violence', 'template_image_url', 'fb_image_url', 'alt_img_urls', 'similar_images', 'similar_images2', 'image_recogn_labels', 'image_recogn_scores', 'search_keywords']]


# ---------------------------------------------

# MAKE TABLE, WHERE AUTHOR IS KEY AND ITS MEMES ARE VALUES
# + SIMILAR  TABLE FOR UPDATERS

# - Sort memes of each author by number of views
# - Add also No. of memes as a column

def make_person_df(df, col):
    df_short = df[df[col].isna()==False] # leave out nan rows
    meme_ids = df_short.index.tolist()
    persons = df_short[col].tolist()
    
    uniques = list(set(persons)) # authors: 5831, updaters: 834
    tmp = df_short['author']
    persons2, memes, counts, tot_views = [], [], [], []
    for p in uniques:
        # p = persons[0]
        df_person = df_short[df_short[col]==p].sort_values(by=['views'], ascending = False)
        persons2.append(p)
        counts.append(len(df_person))
        memes.append(df_person.index.tolist())
        tot_views.append(np.sum(df_person['views']))
    
    df2 = pd.DataFrame(data=[memes, counts, tot_views]).T
    df2.index = persons2
    df2.columns = ['meme_ids', 'meme_counts', 'tot_views']
    
    return df2

df_authors = make_person_df(df, 'author')
df_updaters = make_person_df(df, 'updater')


# ---------------------------------------------

# SAVE ALL:

# Main table in versions:
df_main.to_csv(path_to_cleaned + 'main_df.csv', sep=';', index=True, encoding='utf-8') 

df_main2.to_csv(path_to_cleaned_tmp + 'main_df.csv', sep=';', index=True, encoding='utf-8') 

# Authors and updaters:
df_authors.to_csv(path_to_cleaned_tmp + 'authors_df.csv', sep=';', index=True, encoding='utf-8')
df_updaters.to_csv(path_to_cleaned_tmp + 'updaters_df.csv', sep=';', index=True, encoding='utf-8')   

df_authors.to_json(path_to_cleaned + 'authors.json', force_ascii=False, orient='index') 
df_updaters.to_json(path_to_cleaned + 'updaters.json', force_ascii=False, orient='index') 



