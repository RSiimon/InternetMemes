# -*- coding: utf-8 -*-

# Data 2 - kym_vision.json

import numpy as np
import pandas as pd
import json
from copy import deepcopy
import string

path_to_data = "data//"
path_to_cleaned = "cleaned_data//tmp_with_lists//"

fname = "kym_vision.json"
fname_main_df = 'main_df.csv'
fname_refs = 'refs_df.csv'
fname_rel = 'relations_df.csv'
fname_textual = 'textual_df.csv'

# Loading data
df = pd.read_json(path_to_data + fname, orient='index')
df_main = pd.read_csv(path_to_cleaned + fname_main_df, sep=';', index_col=0, encoding='utf-8')
refs_df = pd.read_csv(path_to_cleaned + fname_refs, sep=';', index_col=0, encoding='utf-8')
relations_df = pd.read_csv(path_to_cleaned + fname_rel, sep=';', index_col=0, encoding='utf-8')
textual_df = pd.read_csv(path_to_cleaned + fname_textual, sep=';', index_col=0, encoding='utf-8')


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


df_main = convert_list_col('similar_images', df_main)
df_main = convert_list_col('type', df_main)
df_main = convert_list_col('search_keywords', df_main)
relations_df = convert_list_col('siblings', relations_df)
relations_df = convert_list_col('children', relations_df)
# type(relations_df['siblings'].iloc[0])  # list

# Convert 'year' column to int:
df_main['year'] = df_main['year'].astype(pd.Int32Dtype())

# Original cleaned data title and url for joining 
id_df = pd.DataFrame(data=df_main.index.tolist(), columns=['url'])
df.index = pd.Series([x.split('/')[-1] for x in df.index.tolist()])

# Leave only rows present in main table:
df = id_df.merge(df, left_on='url', right_index=True)
df.index = df['url'].tolist()
df = df.drop(columns=['url'])
ids = df.index.tolist()

# - There were 19 rows in main_df which were not in df:
dropped = df_main.drop(ids).index.tolist()  # drop later, with others

# df.columns 
# ['labelAnnotations', 'safeSearchAnnotation', 'webDetection', 'error']

# -----------------------------------

# 'ERROR' COLUMN:

# - Rows with bad image data:
err_lab = df['error']
dropped += err_lab.dropna().index.tolist()
df = df.drop(columns=['error'])

# -----------------------------------

# SAFE SEARCH ANNOTATIONS COLUMN:

df_safe = df['safeSearchAnnotation'].apply(pd.Series)
df_safe = df_safe.drop(columns=[0])

# Leave out some disturbing images:
a = df_safe[df_safe['medical'] == "VERY_LIKELY"]  # leave all out (6)
# a = df_safe[df_safe['medical']=="LIKELY"] # mostly ok
# a = df_safe[df_safe['racy']=="VERY_LIKELY"] # mostly ok
# a = df_safe[df_safe['spoof']=="VERY_LIKELY"] " ok
b = df_safe[df_safe['violence'] == "VERY_LIKELY"]  # # leave all out (somehow disturbing) (13)
c = df_safe[df_safe['adult'] == "VERY_LIKELY"]  # leave all out  (23)

dropped += a.index.tolist()
dropped += b.index.tolist()
dropped += c.index.tolist()
dropped = list(set(dropped))  # leave out dumplicates  # len 138

# -----------------------------------

# DROP ROWS WITH BAD IMAGE DATA AND DISTURBING IMAGES FROM ALL TABLES, CONSOLIDATE TABLES SO THAT DF HAS SAME ROWS AS MAIN TABLE:

df = df[df.index.isin(dropped) == False]
df_main = df_main[df_main.index.isin(dropped) == False]
refs_df = refs_df[refs_df.index.isin(dropped) == False]
relations_df = relations_df[relations_df.index.isin(dropped) == False]
textual_df = textual_df[textual_df.index.isin(dropped) == False]
df_safe = df_safe[df_safe.index.isin(dropped) == False]


# In relations_df, also remove dropped ids from siblings/parents/children columns:
def drop_values_parent(df2, dropped):
    parent2 = []
    for row in df2['parent'].tolist():
        p = np.nan
        if isinstance(row, str) and row not in dropped:
            p = row
        parent2.append(p)
    df2['parent'] = parent2
    return df2


relations_df = drop_values_parent(relations_df, dropped)


def drop_values(df2, col, dropped):
    rows2 = []
    for row in df2[col].tolist():
        if isinstance(row, list):
            row2 = []
            for el in row:
                if el not in dropped:
                    row2.append(el)
            if len(row2) == 0:
                row2 = np.nan
        else:
            row2 = np.nan
        rows2.append(row2)
    df2[col] = rows2

    return df2


relations_df = drop_values(relations_df, 'siblings', dropped)
relations_df = drop_values(relations_df, 'children', dropped)

## Checked: Now there are no rows with missing values:
# df.isna().sum()   # 
## labelAnnotations        0
## safeSearchAnnotation    0
## webDetection            0

# Check if main_df and df have same index:
# len(df_main)==len(df) # True
# len(set(df_main.index.tolist()) - set(df.index.tolist()))  # 0 - ok!

# -----------------------------------

# ADD SAFE SEARCH ANNOTATIONS TO MAIN TABLE:
df_main = pd.concat([df_main, df_safe], axis=1)

# for col in df_safe.columns.tolist():
#     print(col, df_safe[col].unique())

# -----------------------------------

# LABEL ANNOTATIONS COLUMN:

# - Contains output from Google Vision API, with the following fields:
#  a) 'mid' - label id
#  b) 'description' - label
#  c) 'score'- confidence score
#  d) 'topicality' - always same as 'score' (at least in our data)

# - Each meme has been annotated with labels indicating which objects the Vision API had detected on the image, with each label being accompanied with confidence score.

# Let's simplify the data structure:
# - Leave out "mid", since using them instead of labels themselves adds no value (labels themselves are also quite short).
# - Leave out "topicality", because it's always the same as "score". 
# - Decrease the level of complexity: for each meme, include a list of labels, and a list of corresoponding confidence scores in decreasing order. 

annotations = df['labelAnnotations'].tolist()
vision_annot_labels = []
vision_annot_scores = []
vision_annot = []
# nan_count = 0
for row in annotations:
    row_labels, row_scores = [], []
    if isinstance(row, list) and len(row) != 0:
        for label in row:
            row_labels.append(label['description'])
            row_scores.append(label['score'])
    # else:
    #     nan_count+=1  # 78 nan values
    vision_annot.append([row_labels, row_scores])
    vision_annot_labels.append(row_labels)
    vision_annot_scores.append(row_scores)

# Add to main table:
df_main['image_recogn_labels'] = vision_annot_labels
df_main['image_recogn_scores'] = vision_annot_scores

# TODO: Save as json (add ids as keys)
# -maybe: add "has_vison_annot" in main table
# + leave out rows that dont have vision annotations (78 rows)

# ---------------

# Reverse annotations:
# - to easily search for memes that have same annotation (label):

# Gather all labels:
all_labels = set()
for row in vision_annot:
    all_labels = all_labels.union(row[0])

all_labels = list(all_labels)  # len 4411

# Add memes to labels with conf. scores:

vision_annot_rev = {}
for label in all_labels:
    # vision_annot_rev[label] = {'memes': [], 'scores': []}
    vision_annot_rev[label] = []  # to allow ordering

for i, row in enumerate(vision_annot):
    if isinstance(row, list) and len(row[0]) != 0:
        annots, scores = row[0], row[1]
        for j in range(len(annots)):
            vision_annot_rev[annots[j]].append([ids[i], scores[j]])

# values =  list(vision_annot_rev.values())
# values2 = []
for key, value in vision_annot_rev.items():
    # el = values[0]
    d = pd.DataFrame(data=value)
    d = d.sort_values(by=[1], ascending=False).T
    # values2.append([d.loc[0].tolist(), d.loc[1].tolist()])
    vision_annot_rev[key] = [d.loc[0].tolist(), d.loc[1].tolist()]
    # vision_annot_rev[key]['memes'] = d.loc[0].tolist()
    # vision_annot_rev[key]['scores'] =  d.loc[1].tolist()

# Split into two dicts:
a = {}  # deepcopy(vision_annot_rev)
b = {}  # deepcopy(vision_annot_rev)
for key, value in vision_annot_rev.items():
    a[key] = value[0]
    b[key] = value[1]

a = pd.DataFrame(list(a.items()))
b = pd.DataFrame(list(b.items()))
a.index = a[0]
b.index = b[0]
a = a.drop(columns=[0])
b = b.drop(columns=[0])
vision_annot_rev_df = pd.concat([a, b], axis=1)
vision_annot_rev_df.columns = ['memes', 'scores']

# -----------------------------------


# WEB DETECTION COLUMN:

df_web = df['webDetection'].apply(pd.Series)

df_web.columns
# ['bestGuessLabels', 'fullMatchingImages', 'pagesWithMatchingImages',
#        'partialMatchingImages', 'visuallySimilarImages', 'webEntities']


# ----------------

# - BEST GUESS LABELS

wlab = df_web['bestGuessLabels'].tolist()
labels = []
lang_codes = []

for row in wlab:
    if 'label' in row[0].keys():
        labels.append(row[0]['label'])
    else:
        labels.append(np.nan)
    if 'languageCode' in row[0].keys():
        lang_codes.append(row[0]['languageCode'])
    else:
        lang_codes.append(np.nan)

df_web['best_guess_label'] = labels
df_web['best_guess_lang'] = lang_codes
df_web = df_web.drop(columns=['bestGuessLabels'])

# LEAVE OUT MEMES WITH NON-ENGLISH LANGUAGE CODE:

# - Often those memes were related to some public figures of specific countries. The language of the text on the meme was still in most cases English.
# -> We decide to leave them out, since most people lack knowledge about country-specific context, and would not understand the meme without reading the description. 

non_english = []
ids = df.index.tolist()
for i, el in enumerate(df_web['best_guess_lang'].tolist()):
    if isinstance(el, str) == True and el != 'en':
        non_english.append(ids[i])

dropped = non_english

df = df[df.index.isin(dropped) == False]
df_main = df_main[df_main.index.isin(dropped) == False]
refs_df = refs_df[refs_df.index.isin(dropped) == False]
relations_df = relations_df[relations_df.index.isin(dropped) == False]
textual_df = textual_df[textual_df.index.isin(dropped) == False]
df_safe = df_safe[df_safe.index.isin(dropped) == False]
vision_annot_rev_df = vision_annot_rev_df[vision_annot_rev_df.index.isin(dropped) == False]

relations_df = drop_values_parent(relations_df, dropped)
relations_df = drop_values(relations_df, 'siblings', dropped)
relations_df = drop_values(relations_df, 'children', dropped)

df_web = df_web[df_web.index.isin(dropped) == False]
df_web = df_web.drop(columns=['best_guess_lang'])

# From relations_df, Leave out any rows where there are now only nans:
a = np.sum(relations_df.isna(), axis=1)
a = a[a == 3].index.tolist()
relations_df = relations_df.dropna(axis=0, how='all')
for el in a:
    df_main['has_relations'].loc[a] = False

# ADD BEST GUESS LABEL TO SEARCH KEYWORDS:
df_main['best_guess_label'] = df_web['best_guess_label']
# a = df_main[['type', 'search_keywords', 'title', 'best_guess_label']]
search = df_main['search_keywords'].tolist()
best_guess = df_main['best_guess_label'].tolist()

for i, row in enumerate(best_guess):
    if isinstance(row, str) == True:
        search_row = search[i]
        if len(search_row) == 1 and search_row[0] == "":
            search[i] = [row]
        else:
            search[i].append(row)

df_main['search_keywords'] = search
df_main = df_main.drop(columns=['best_guess_label'])

# ----------------

# - FULL MATCHING IMAGES and PARTIAL MATCHING IMAGES:

# 'Full matching images' are alternative urls of the same image all over the web (including KYM website, which we already have). KYM often has same image in different sizes (original, mobile, facebook). Those links might become important, because queries to KYM website seem to be limited and making too many queries per minute may result in blocking of IP (it already once happened for us). 

# 'Partial matching images' seem also to be alternative urls to the same image with minor differences (sometimes with an additional text/caption, or slightly different text, or somewhat lower quality). Basically, they are still the same memes. 

# In main table, we already have two image urls to KYM website, with no missing values:
# df_main['template_image_url'].isna().sum()  # no missing values 
# df_main['fb_image_url'].isna().sum() # no missing values

# -> 'Full matching images': Save first six alternative urls that are not to KYM website. (Some memes don't have image urls here, so we will have missing values.).  

# -> Add partially matching images to image urls in cases where we have less than six alternative image urls. Again, exclude urls to KYM (we already have those). 

df_wmatch = df_web['fullMatchingImages'].tolist()
df_wmatch3 = df_web['partialMatchingImages'].tolist()


def collect_urls(df2, include_kym, max_urls):
    # df2 = wsim
    urls = []
    for i, row in enumerate(df2):
        # i = 0
        # row = df2[0]
        if isinstance(row, list) == True and len(row) != 0:
            counts = 0
            row_urls = []
            for el in row:  # dict
                # el = row[0]
                link = el['url']
                if include_kym == False:
                    if "kym-cdn" not in link:
                        row_urls.append(link)
                        counts += 1
                else:
                    row_urls.append(link)
                    counts += 1
                if counts == max_urls:
                    break
            if len(row_urls) != 0:
                urls.append(row_urls)
            else:
                urls.append(np.nan)
        else:
            urls.append(np.nan)
    return urls


urls = collect_urls(df_wmatch, False, 6)
urls2 = collect_urls(df_wmatch3, False, 6)
urls3 = []

for i, row in enumerate(urls):
    if isinstance(row, list) == False:
        urls3.append(urls2[i])
    elif len(row) >= 6:
        urls3.append(row)
    else:
        # break
        urls3.append(row + urls2[:6 - len(row)])

# ids = df.index.tolist()


df_main['alt_img_urls'] = urls
df_web = df_web.drop(columns=['fullMatchingImages', 'partialMatchingImages'])

# ----------------

# PAGES WITH MATCHING IMAGES:

# df_wmatch2 = df_web['pagesWithMatchingImages'].tolist()
# -> pages where matching images were found. NOT RELEVANT -> LEAVE OUT
df_web = df_web.drop(columns=['pagesWithMatchingImages'])

# ----------------

# - VISUALLY SIMILAR IMAGES:  -> REALLY USEFUL!

# -> These are derivatives of the original meme; similar, but not the same.

wsim = df_web['visuallySimilarImages'].tolist()
# sim = df_main['similar_images'].tolist() # another set of similar images

# Add max 10 similar images (only six had more than 10):
urls4 = collect_urls(wsim, True, 10)
df_main['similar_images2'] = urls4
df_web = df_web.drop(columns=['visuallySimilarImages'])

a = df_main.isna().sum()

# ----------------

# WEB ENTITIES:

# Here, we have keywords with scores. The keywords describe image category rather than what is on the image. 

# -> leave out scores. 

web_entities = df_web['webEntities'].tolist()
types = df_main['type'].tolist()

# Collect keyowrds:
keywords = []
for i, row in enumerate(web_entities):
    if isinstance(row, list) == True and len(row) != 0:
        row_keywords = []
        for el in row:
            if 'description' in el.keys():
                row_keywords.append(el['description'])
        if len(row_keywords) != 0:
            keywords.append(row_keywords)
        else:
            keywords.append(np.nan)
    else:
        keywords.append(np.nan)

df_main['type2'] = keywords

# -----------------------------------

# SAVE ALL:

df_main.to_csv(path_to_cleaned + 'main_df.csv', sep=';', index=True, encoding='utf-8')
refs_df.to_csv(path_to_cleaned + 'refs_df.csv', sep=';', index=True, encoding='utf-8')
relations_df.to_csv(path_to_cleaned + 'relations_df.csv', sep=';', index=True, encoding='utf-8')
textual_df.to_csv(path_to_cleaned + 'textual_df.csv', sep=';', index=True, encoding='utf-8')
vision_annot_rev_df.to_csv(path_to_cleaned + 'vision_annot_df.csv', sep=';', index=True, encoding='utf-8')

# Load data:
# df = pd.read_csv('main_df.csv', sep=';', index_col=0, encoding='utf-8')       


# -----------------------------------
