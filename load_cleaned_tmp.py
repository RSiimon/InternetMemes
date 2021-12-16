# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import json 
import datetime
from datetime import date

path_to_data = "data//"  
path_to_cleaned = "cleaned_data//"
path_to_cleaned_tmp = "cleaned_data//tmp_with_lists//"

# in cleaned_data//tmp_with_lists:
fname_main_df = 'main_df.csv'
fname_refs = 'refs_df.csv'
fname_rel = 'relations_df.csv'
fname_textual = 'textual_df.csv'
fname_vision = 'vision_annot_df.csv'
fname_DBpedia = 'DBpedia_df.csv'
fname_authors = 'authors_df.csv'
fname_updaters = 'updaters_df.csv'

# in cleaned_data:
fname2_main_df = 'main_df.csv'
fname2_refs = 'refs_df.csv'
fname2_textual = 'textual_df.csv'
fname2_relations = 'relations.json'  # orient='index'
fname2_vision = 'vision_annot.json' # orient='index'
fname2_DBpedia = 'DBpedia.json' # orient='index'
fname2_similar_img ='similar_img_urls.json' # orient='index'
fname2_image_recogn ='image_recogn.json' # orient='index'
fname2_alternative_urls ='alternative_urls.json' # orient='columns'
fname2_search_keywords ='search_keywords.json' # orient='columns'
fname2_authors = 'authors.json'  # orient='index'
fname2_updaters = 'updaters.json'  # orient='index'



# LOAD DATA: 

# OPTION 1: LOAD DATAFRAMES CONTAINING LISTS:
    
# Loading data
df_main = pd.read_csv(path_to_cleaned_tmp + fname_main_df, sep=';', index_col=0, encoding='utf-8') 
refs_df= pd.read_csv(path_to_cleaned_tmp + fname_refs, sep=';', index_col=0, encoding='utf-8')
relations_df= pd.read_csv(path_to_cleaned_tmp + fname_rel, sep=';', index_col=0, encoding='utf-8')
textual_df = pd.read_csv(path_to_cleaned_tmp + fname_textual, sep=';', index_col=0, encoding='utf-8')
vision_annot_rev_df = pd.read_csv(path_to_cleaned_tmp + fname_vision, sep=';', index_col=0, encoding='utf-8')
DBpedia_df = pd.read_csv(path_to_cleaned_tmp + fname_DBpedia, sep=';', index_col=0, encoding='utf-8')
authors_df = pd.read_csv(path_to_cleaned_tmp + fname_authors, sep=';', index_col=0, encoding='utf-8')
updaters_df = pd.read_csv(path_to_cleaned_tmp + fname_updaters, sep=';', index_col=0, encoding='utf-8')

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

df_main = convert_list_col('similar_images', df_main) 
df_main = convert_list_col('similar_images2', df_main) 
df_main = convert_list_col('image_recogn_labels', df_main) 
df_main = convert_list_col('image_recogn_scores', df_main) 
df_main = convert_list_col('alt_img_urls', df_main) 
df_main = convert_list_col('search_keywords', df_main) 
relations_df = convert_list_col('siblings', relations_df) 
relations_df = convert_list_col('children', relations_df) 
vision_annot_rev_df = convert_list_col('memes', vision_annot_rev_df) 
vision_annot_rev_df = convert_list_col('scores', vision_annot_rev_df) 
DBpedia_df= convert_list_col('scores', DBpedia_df) 
DBpedia_df = convert_list_col('keywords', DBpedia_df) 
DBpedia_df = convert_list_col('links', DBpedia_df) 
authors_df = convert_list_col('meme_ids', authors_df) 
updaters_df = convert_list_col('meme_ids', updaters_df) 
# type(relations_df['siblings'].iloc[0])  # list

df_main = convert_date_col('added', df_main) 
df_main = convert_date_col('updated', df_main) 
## type(df_main['added'].iloc[0]) # datetime.date

## Convert 'year' column to int:
df_main['year'] = df_main['year'].astype(pd.Int32Dtype()) 
## type(df_main['year'].iloc[0]) 
 

# -----------------------------------------------

# # OVERVIEW OF ALL COLUMNS:   (dtypes and missing values)

# Missing values and types:
def make_cols_overview(df2):
    missing, types = [], []
    for col in df2.columns.tolist():
        types.append(str(type(df2[col].iloc[0])).split("'")[1])
        missing.append(df2[col].isna().sum())
    
    counts_df = pd.DataFrame(data=[df2.columns.tolist(), types, missing]).T
    counts_df.columns = ['column', 'type', 'missing']
    print(len(df2), 'rows, ', len(df2.columns.tolist()), 'columns')
    
    return counts_df

counts_main = make_cols_overview(df_main) # 11610 rows,  24 columns
counts_refs = make_cols_overview(refs_df) # 3655 rows,  7 columns
counts_relations = make_cols_overview(relations_df) # 4672 rows,  3 columns
counts_textual = make_cols_overview(textual_df) # 6935 rows,  3 columns
counts_vision = make_cols_overview(vision_annot_rev_df) # 4410 rows,  2 columns
counts_DBpedia = make_cols_overview(DBpedia_df) # 3539 rows, 3 columns
counts_authors = make_cols_overview(authors_df) # 5831 rows, 3 columns
counts_updaters = make_cols_overview(updaters_df) # 834 rows, 3 columns 

    
# -----------------------------------------------

# OPTION 2: LOAD FINAL DATAFRAMES (WITHOUT LISTS) AND JSONS:
    
    
# A) LOAD  DATAFRAMES:

df_main = pd.read_csv(path_to_cleaned + fname2_main_df, sep=';', index_col=0, encoding='utf-8') 
refs_df= pd.read_csv(path_to_cleaned + fname2_refs, sep=';', index_col=0, encoding='utf-8')
textual_df = pd.read_csv(path_to_cleaned + fname2_textual, sep=';', index_col=0, encoding='utf-8')

# Convert 'year' column to int:
df_main['year'] = df_main['year'].astype(pd.Int32Dtype()) 
## type(df_main['year'].iloc[0]) 

# Convert dates from str to datetime:
df_main = convert_date_col('added', df_main) 
df_main = convert_date_col('updated', df_main) 

    
# B) LOAD  JSONS AS DICTS:

def load_json2(path_to_cleaned, fname):
	with open(path_to_cleaned + fname, 'r', encoding = 'utf-8') as f:
		for row in f:
			my_dict = json.loads(row)
		return my_dict


relations_dict = load_json2(path_to_cleaned, fname2_relations)
vision_dict=  load_json2(path_to_cleaned, fname2_vision)
DBpedia_dict=  load_json2(path_to_cleaned, fname2_DBpedia)
similar_img_dict=  load_json2(path_to_cleaned, fname2_similar_img)
image_recogn_dict=  load_json2(path_to_cleaned, fname2_image_recogn)
alternative_urls_dict=  load_json2(path_to_cleaned, fname2_alternative_urls)['alt_img_urls']
search_keywords_dict= load_json2(path_to_cleaned, fname2_search_keywords)['search_keywords']
authors_dict = load_json2(path_to_cleaned, fname2_authors)
updaters_dict = load_json2(path_to_cleaned, fname2_updaters)

# -------------------------------------------

# B.1) TO LOAD JSONS AS DATAFRAMES: 
    
# relations_df = pd.read_json(path_to_cleaned+ fname2_relations, orient='index') 
# vision_df = pd.read_json(path_to_cleaned+ fname2_vision, orient='index')
# DBpedia_df = pd.read_json(path_to_cleaned+ fname2_DBpedia, orient='index')
# similar_img_df = pd.read_json(path_to_cleaned+ fname2_similar_img, orient='index')
# image_recogn_df = pd.read_json(path_to_cleaned+ fname2_image_recogn, orient='index')
# alternative_urls_df = pd.read_json(path_to_cleaned+ fname2_alternative_urls, orient='columns')
# search_keywords_df = pd.read_json(path_to_cleaned+ fname2_search_keywords, orient='columns')
# authors_df = pd.read_json(path_to_cleaned+ fname2_authors, orient='index')
# updaters_df = pd.read_json(path_to_cleaned+ fname2_updaters, orient='index')



