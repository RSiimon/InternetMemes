# -*- coding: utf-8 -*-

# Data 1 - kym.json

import numpy as np
import pandas as pd
from copy import deepcopy 
import json 
import datetime
from datetime import date
import re
import os


path_to_data = "data//"  
path_to_cleaned = "cleaned_data//tmp_with_lists//"
path_to_cleaned2 = "cleaned_data//"
fname = "kym.json"

# ------------------------------------

# Make folders for cleaned data if they dont exist:
if path_to_cleaned2[:-2] not in os.listdir():
    os.mkdir(path_to_cleaned2)
    os.mkdir(path_to_cleaned)
    
# ------------------------------------

# Loading data
with open(path_to_data + fname) as f:
    data = json.load(f)

df = pd.DataFrame(data)

# Selecting only memes
df = df.set_index(['category'])
df = df.loc['Meme'].reset_index()

# Dropping unneccessary columns
df = df.drop(columns=['category'])

# Dropping duplicates
df = df.drop_duplicates(subset=['title', 'url']) # korras df - v.a jargmised kolumnid 

# Dropping memes with bad-words and sensitive content
drop_values = ['fuck', 'shit', 'dick', 'Fuck', 'Shit', 'Dick', 'FUCK', 'penis', 'Penis', 'PENIS', 'Gay', 'gay', 'GAY', 'Naked', 'poop', 'Poop', 'Nigga', 'Nigger', 'nigga', 'Suck', 'Cock', 'Porn', 'porn', 'Ass', 'Raped', 'raped', 'Rape', 'Pussy', 'Cunt', 'Bitch']
df = df[~df['title'].str.contains('|'.join(drop_values))].reset_index()
df = df.drop(columns=['index']) # drop old index column # added
# df.index = df['url'].tolist()

## Drop a few urls consisting mostly of symbols (usually these were links to non-English pages):
ids_short = pd.Series([x.split('/')[-1] for x in df['url'].tolist()])
dropped =  ids_short.sort_values()[:16].index.tolist() 
dropped+=[12122, 10998]
df = df.drop(dropped)

# Drop 6 memes with no autors (i.e where server returned code 404 to web crawler); checked those 6 pages and indeed they were no longer available:
dropped2=['https://knowyourmeme.com/memes/temmie', 
          'https://knowyourmeme.com/memes/its-all-mtv', 
          'https://knowyourmeme.com/memes/x-sells-everything-to-y', 
          'https://knowyourmeme.com/memes/ap-gibralter', 
          'https://knowyourmeme.com/memes/chili-can-be-served-with-cheese', 
          'https://knowyourmeme.com/memes/hyakugojyuuichi--2'] 

# Leave out memes with no description (added later):
dropped2+=['https://knowyourmeme.com/memes/tequila-heineken-pas-ltemps-dniaiser', 
           'https://knowyourmeme.com/memes/schismatic-pony', 
           'https://knowyourmeme.com/memes/gaul-laughing-stock', 
           'https://knowyourmeme.com/memes/texting-etiquette', 
           'https://knowyourmeme.com/memes/got-my-nugs', 
           'https://knowyourmeme.com/memes/dj-jesus-the-god-of-electronic-music', 
           'https://knowyourmeme.com/memes/theflyingcamel', 
           'https://knowyourmeme.com/memes/beauty-pageant-reaction', 
           'https://knowyourmeme.com/memes/boom-boom-boom-mohammed', 
           'https://knowyourmeme.com/memes/van-persie-fall', 
           'https://knowyourmeme.com/memes/bad-romance-fail', 
           'https://knowyourmeme.com/memes/my-foot', 
           'https://knowyourmeme.com/memes/azerbaijan-dance']

df = df[df['url'].isin(dropped2)==False]


ids_short = pd.Series([x.split('/')[-1] for x in df['url'].tolist()])

# Saving id column for joining:
id_df = df[["url"]] 

# df = df.drop(columns =['url'])


# -----------------------------------------------

# EXTRACT DATES FROM "LAST_UPDATE_SOURCE" AND "ADDED" COLUMNS:

# Last updated:
df['updated'] = df.apply(lambda row: date.fromtimestamp(row['last_update_source']), axis = 1)
df = df.drop(columns = ['last_update_source'])

# Added (contains nans):
added = df['added'].tolist()
added2 = []
for el in added:
    if np.isnan(el)==True:
        added2.append(el)
    else:
        added2.append(date.fromtimestamp(el))
df['added'] = added2

# -----------------------------------------------

# # CHECK STATUS:
# statuses = df['status'].unique()
# # ['confirmed', 'deadpool', 'submission', 'unlisted']

# # a = df[df['status']=='unlisted']
# a = df[df['status']=='deadpool'] # Pages of those memes carried a red banner "This entry has been rejected due to incompleteness or lack of notability."


# TODO: LEAVE OUT MEMES WITH STATUS = "DEADPOOL" ? (need to decide)
# There are quite many of them (4313)...

# TODO: drop "status" column (after deciding about deadpool, this column is no longer needed)

# -----------------------------------------------
# EXTRACTING STACKED COLUMNS: 'ld', 'meta', 'details', 'content', 'additional_references'):
# -----------------------------------------------

# 'META' COLUMN:
    
# Leave only columns that do not overlap with others (either in main table or in meta table), leaving also out columns containing only a single value:

meta_df = df['meta'].apply(pd.Series) 
meta_df = pd.concat([id_df, meta_df], axis=1)

df['description'] = meta_df['description']
df['fb_image_url'] = meta_df['og:image']
df = df.drop(columns=['meta'])

# -----------------------------------------------

# 'DETAILS' COLUMN:
    
# All columns except 'origin' were relevant - add to main table:
details_df = df['details'].apply(pd.Series) 
details_df = pd.concat([id_df, details_df], axis=1) 

df['year'] = details_df['year']
# df['origin'] = details_df['origin']
df['type'] = details_df['type']
df['status'] = details_df['status']
df = df.drop(columns=['details'])  

# convert year from string to int:
df['year'] = df['year'].astype('float').astype(pd.Int32Dtype()) 
# (simple 'nan' cannot be used in int columns)
# b = np.isnan(df['year']) # ok

# -----------------------------------------------

# 'LD' COLUMN - LEAVE OUT:
# Contains redundant info: category (eg "Meme") and link to meme, which we also have in other columns.
df = df.drop(columns = ['ld'])

# -----------------------------------------------

# 'ADDITIONAL REFERENCES' COLUMN:

# 100+ sites of reference, mostly 1-2 memes for each. 

# - Include the most frequent references (Wikipedia, Urban Dictionary, Encyclopedia Dramatica, Meme Generator, Reddit, Twitter) separately.
# - Among others, add the first one as 'other_ref" (only 30 memes had 2 other references, the rest had less, so not much info is lost). 
# - Exclude values that are not links or include links to non-English sites, and combine keys that have various spellings.
    
def check_if_link(key, value):
    exluded = ["facebook", "translate", "uncyclopedia", "youtu.be", "/nl.", "/es.", "/fr.", "/zh.", "/zh-", "/sv.", "/da.", "/pt.", "/au.", "/hu.", "/it.", "/no.", "/ja.", "/id.", "/fi.", "/de.", "/cs.", ".ch/", ".nl/", ".es/", ".fr/", ".zh/", ".sv/", ".pt/", ".no/", ".ja/", ".id/", ".fi/", ".de/", ".cs/", ".au/", ".it/", ".hu/", ".da/", " "]
    if value.startswith("http") or value.startswith("www") or ".org" in value or ".com" in value or ".net" in value or "encyclopediadramatica.wiki" in value:
        for el in exluded:
            if el in value:
                return False
        if value.startswith("pt.") or len(value)<8:
            return False
        return True
    return False    

refs = df['additional_references'].tolist()
refs2 = [] 

for ref in refs:
    main_refs = [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
    for key, value in ref.items():
        if check_if_link(key, value)==True and key!='Facebook':
            if "wikipedia" in value:
                main_refs[0] = value
            elif "urbandictionary" in value:
                main_refs[1] = value
            elif "encyclopediadramatica" in value:
                main_refs[2] = value            
            elif "memegenerator" in value:
                main_refs[3] = value   
            elif "reddit" in value:
                main_refs[4] = value 
            elif "twitter" in value:
                main_refs[5] = value 
            elif isinstance(main_refs[6], str)==False: 
                main_refs[6] = value # only add 1st other ref
    refs2.append(main_refs)
              
refs_df = pd.DataFrame(data = refs2, columns = ["ref_wikipedia", "ref_urban_dict", "ref_encyc_dramatica", "ref_meme_generator", "ref_reddit", "ref_twitter", "ref_other"], index = df['url'].tolist())

# # Add to main table:
# df = pd.concat([df, refs_df], axis=1)
# df = df.drop(columns=['additional_references'])

# Leave out rows that have no links (only 3844 do have links); 
# also add a column to main table indicating if meme has references.
refs_df['has_refs'] = refs_df.isna().sum(axis = 1)!=7
df['has_refs'] = refs_df['has_refs'].tolist()
refs_df = refs_df[refs_df['has_refs']==True] # len 3844
refs_df = refs_df.drop(columns=['has_refs'])
df = df.drop(columns=['additional_references'])

# Note: adding links based on text of the link itself allowed to avoid having to check multiple spelling variants of the keys (eg "Twitter" vs "Twitter,") and also to correct misclassifications (eg key was "Twitter" but link was to Wikipedia).

# -----------------------------------------------

# 'TYPE' COLUMN  (can be used for finding similar memes):

# Total values and unique values:
def stats_tags(col):
    col_list = df[col].tolist()
    all_list = []
    for el in col_list:
        if isinstance(el, list) and len(el)!=0: 
            all_list+=el
    uniques = set(all_list)
    print('total words:', len(all_list), ', unique words:', len(uniques))
    return pd.Series(all_list).value_counts()

# a = stats_tags('type')
# total words: 3669 , unique words: 34 # -> looks good: few unique types

# Extract types from web link:
types = df['type'].tolist()
types2 = []
for types_list in types:
    if isinstance(types_list, list) and len(types_list)!=0:
        types_list2 = []
        for t in types_list:
            t = t.split('/')[-1] # 'conspiracy-theory'
            t = re.sub('-', ' ', t)
            types_list2.append(t)  # 'conspiracy theory'
        types2.append(types_list2)
    else:
        types2.append(types_list)
    
df['type'] = types2


# -----------------------------------------------

# CLEAN KEYWORD COLUMN:

# - remove "//" and double quotes;
# - remove some non-words (eg ub4dcub77cuad70, etc)

excluded_starts = ['uba3', 'ub4', 'uac', 'u9', 'u8', 'u7', 'u6', 'u5', 'u4', 'u3', 'u2', 'u1', 'u0', '/m/', '(u', '#u', '^', '!1']
keywords = df['search_keywords'].tolist()
keywords2 = []

for keywords_list in keywords:
    keywords_list2=[]
    if isinstance(keywords_list, list) and len(keywords_list)!=0:
        for t in keywords_list:   
            t = re.sub('\\\\', '', t)
            t = re.sub('"', '', t)
            t = re.sub('\'', '', t)
            t = re.sub('#', '', t)
            t = re.sub(',geo:,time:all},{keyword:', '', t)
            is_excluded = False
            for el in excluded_starts:
                if t.startswith(el):
                    is_excluded = True
                    break
            if is_excluded==False:
                keywords_list2.append(t)
    keywords2.append(keywords_list2)

df['search_keywords'] = keywords2
    

# -----------------------------------------------

# CLEAN TAGS COLUMN:
    
# - Remove some frequent tags that cannot be used for search:
    
excluded = ['4chan', 'youtube', 'image macro', 'twitter', 'meme', 'photoshop', 'tumblr', 'video', 'reddit', 'facebook', 'youtube poop', 'hashtag', 'image', 'gif', 'racism', 'memes', 'sex', 'you', 'photo', 'of', 'i', 'image macros', 'google', 'is', 'youtube', 'comment', 'comments']
tags = df['tags'].tolist()
tags2 = []

for tags_list in tags:
    tags_list2=[]
    if isinstance(tags_list, list) and len(tags_list)!=0:
        for t in tags_list:   
            is_excluded = False
            for el in excluded:
                if t==el:
                    is_excluded = True
                    break
            if is_excluded==False:
                tags_list2.append(t)
    tags2.append(tags_list2)

df['tags'] = tags2

# -----------------------------------------------

# MERGE 'TAGS' AND 'SEARCH_KEYWORDS' COLUMNS:  (can be used for seacrh)

# #  -----
# # Some stats before merging:
    
# a = stats_tags('tags')
# # total words: 66678 , unique words: 30165

# b = stats_tags('search_keywords') 
# # total words: 7312 , unique words: 7199
# # -> Only five values in "search_keywords" appeared more than twice. 

# tags_df = df[['tags', 'search_keywords']]
# # a.isna().sum()  # tags- 0 nans, search_keywords - 6751 nans
# lens = [len(x) for x in df['tags'].tolist()]
# b = pd.Series(lens).value_counts()
# # ->  # 792 have no tags, max tags is 50

# #  -----

# Merge 'tags' and 'search_keywords':
 # - also exclude repetitions:
tags = df['tags'].tolist()
keywords = df['search_keywords'].tolist()
keywords2 = []
for i in range(len(tags)):
    keywords2.append(list(set(tags[i] + keywords[i])))

# lens = [len(x) for x in keywords2]
# b = pd.Series(lens).value_counts()
# # ->  # 746 have no keywords, max keywords is 50

df['search_keywords'] = keywords2
df = df.drop(columns=['tags'])

# -----------------------------------------------

# 'CONTENT' COLUMN:
    
content = df['content'].tolist()
ids = df['url'].tolist()

about = ['about', 'about:', 'about.', 'about & origin', 'about and origin', 'origin/about']
origin = ['origin', 'origins', 'origin:', 'orgin', 'origin and character', 'origin and spread', 'the origin', 'origins:', 'search for the origin', 'origins/early development', 'origin.', 'origins and spread']
spread = ['spread', 'popularity', 'in popular culture', 'spread and popularity', 'popular culture', 'popularity and spread', 'viral spread', 'spread & popularity', 'mainstream coverage and declining popularity', 'impact', 'fandom', 'online presence']
search_interest = ['search interest', 'search', 'search interests', 'trend', 'search trends', 'google insights for search', 'search trend', 'search analytics', 'search interest:', 'search insight', 'google search interest', 'google insight', 'google insights', 'insight', 'notable developments']
similar_images = ['examples', 'various examples',  'notable examples', 'notable images', 'variations', 'notable developments', 'derivatives', 'derivative works', 'direct derivatives'] # (some are pics from other sites) 

def make_content_col(keyword_list, content, img_or_txt):
    keyword_col = []
    for row in content:
        if img_or_txt=="text":
            texts = ""
        else:
            texts = []
        if isinstance(row, dict) and len(row)!=0:
            keys = list(row.keys())
            for key in keys:
                if key in keyword_list:
                    if img_or_txt in row[key]:
                        keyword_texts = row[key][img_or_txt]
                        if img_or_txt=="text":
                            for text in keyword_texts:
                                if len(re.findall("in progress", text.lower()))==0 and len(re.findall("not available", text.lower()))==0:
                                    texts+=text+"\n"
                        else:
                            texts+=keyword_texts
                    elif "subsections" in row[key]:
                        subsect_dicts = list(row[key]["subsections"].values())
                        for subsect_dict in subsect_dicts:
                            if img_or_txt in subsect_dict:
                                keyword_texts = subsect_dict[img_or_txt]
                                if img_or_txt=="text":
                                    for text in keyword_texts:
                                       if len(re.findall("in progress", text.lower()))==0 and len(re.findall("not available", text.lower()))==0:
                                            texts+=text+"\n"
                                else:
                                    texts+=keyword_texts                            
        if len(texts)>0:
            keyword_col.append(texts)
        else: 
            keyword_col.append(np.nan) # or maybe " " ?
    return keyword_col


about_col = make_content_col(about, content, "text") 
origin_col = make_content_col(origin, content, "text") 
spread_col = make_content_col(spread, content, "text") 
search_interest_col = make_content_col(search_interest, content, "text")
similar_images_col = make_content_col(similar_images, content, "images")


# Remove references ([1], [2], ...) from text columns:

# -> Those are references (mostly links, sometimes explanations) used in the text, in the form: [1] .... , [2] ......, etc. We exclude them, because we will not allow user to make queries to all possible websites. 

def remove_refs(col):
    col2 = []
    for row in col: 
        if isinstance(row, str) and len(row)!=0:
            col2.append(re.sub('\[[0-9]*\]', '', row))
        else:
            col2.append(np.nan)
    return col2

about_col = remove_refs(about_col)
origin_col = remove_refs(origin_col)
spread_col = remove_refs(spread_col) 
search_interest_col = remove_refs(search_interest_col)
# similar_images_col


# # How many rows have data?
# cols = [about_col, origin_col, spread_col, search_interest_col, similar_images_col]
# colnames = ['about', 'origin', 'spread', 'search_interest', 'similar_images']

# for i, col in enumerate(cols):
#     rows_with_data = len(df) - pd.Series(col).isna().sum()
#     print(colnames[i], ':', rows_with_data, 'rows have data')
# # about : 7341 rows have data
# # origin : 7038 rows have data
# # spread : 6481 rows have data
# # search_interest : 805 rows have data
# # similar_images : 3142 rows have data

df['about'] = about_col
df['origins'] = origin_col # We already have an "origin" column, with different contents
df['spread'] = spread_col 
df['search_interest'] = search_interest_col 
df['similar_images'] = similar_images_col
df = df.drop(columns = ['content'])


#-------------

# DIDN'T INCLUDE:
# - external_links = ['external links', 'external references.', 'external reference', 'external references', 'references', 'external references:', 'external refereces', 'other references', 'media references', 'external references', 'resources', 'external resources', 'external refrences', 'external sources']

# - 'related memes' - often not that similar at all. 

# -----------------------------------------------

# MERGE 'DESCRIPTION' AND 'ABOUT' COLUMN:
    
# - If there was text in both columns, it often partially or fully overlapped. So it's better to merge them.
# -Merging policy: if both columns have text, leave text that is longer. This helps get rid of cases where description only consists of one or a couple of words instead of proper description. 

desc = df['description'].tolist()
# len(df) - pd.Series(desc).isna().sum() # 12164 rows have data

desc2 = []
for i in range(len(df)):
    text_desc = desc[i]
    text_about = about_col[i]
    if isinstance(text_about, str)==False: # description missing
        desc2.append(text_desc)
    elif isinstance(text_desc, str)==False: # about missing
        desc2.append(text_about)
    else:
        if len(text_desc)>len(text_about):
            desc2.append(text_desc)
        else:
            desc2.append(text_about)
        
# len(df) - pd.Series(desc2).isna().sum() # 12164 rows have data
df['description'] = desc2
df = df.drop(columns = ['about'])

# -----------------------------------------------

# REPLACE ID COLUMN: (to make ids shorter)

# - eg. "in-soviet-russia" instead of "https://knowyourmeme.com/memes/in-soviet-russia" (beginning of url is the same for all memes)

ids_short = pd.Series([x.split('/')[-1] for x in df['url'].tolist()])
ids_short = ids_short.tolist()
df.index = ids_short

refs_ids_short = pd.Series([x.split('/')[-1] for x in refs_df.index.tolist()])
refs_ids_short =refs_ids_short.tolist()
refs_df.index = refs_ids_short
# id_df = df['url']

# -----------------------------------------------

# Move 'origins', 'spread' and 'search_interest' into separate table:
textual_df = df[['origins', 'spread', 'search_interest']]
textual_df = textual_df.dropna(axis=0, how = 'all')

a = pd.Series(df.index)
b = a.isin(textual_df.index.tolist())
df['has_other_texts'] = b.tolist()
df = df.drop(columns = ['origins', 'spread', 'search_interest'])


# -----------------------------------------------

# PARENT, SIBLINGS AND CHILDREN:

# - Check if parent/sibling/child is in table; if not, leave out.
# - Replace link with id.
# - Remove memes own name from the list of its siblings / children. 
parent = df['parent'].tolist()
sib = df['siblings'].tolist()
children = df['children'].tolist()

# Replace links with ids and remove if not in table:
parent2  =[]
for row in parent:
    if isinstance(row, str): #  and len(row)!=0
        name = row.split('/')[-1]
        if name in ids_short:
            parent2.append(name)
        else:
            parent2.append(np.nan)
    else:
        parent2.append(np.nan)


def check_names(col, ids_short):
    col2=[]
    for i, row in enumerate(col):
        if isinstance(row, list) and len(row)!=0:
            names_list = []
            for name in row:
                name = name.split('/')[-1]
                if name in ids_short and name!=ids_short[i]:
                    names_list.append(name)
            if len(names_list)!=0:
                col2.append(names_list)
            else:
                col2.append(np.nan)
        else:
            col2.append(np.nan)
    return col2

sib2 = check_names(sib, ids_short)   
children2 = check_names(children, ids_short)


df['parent'] = parent2
df['siblings'] = sib2
df['children'] = children2


# ------------------------
    
# - For each sibling, check if other siblings have this meme also among its siblings; if not, then add:

sib2 = pd.Series(sib2, index = ids_short)
children2 = pd.Series(children2, index = ids_short)
parent2 = pd.Series(parent2, index = ids_short)

# sib2_copy = deepcopy(sib2)
# sib2 = deepcopy(sib2_copy)

def add_siblings(sib2, ids_short):
    # changes, changes2 = 0, 0
    sib_list = deepcopy(sib2.tolist()) # 
    for i, row in enumerate(sib_list):
        if isinstance(row, list) and len(row)!=0: # if has siblings
            name = ids_short[i]
            for sib in row:
                sib_row = sib2.loc[sib]
                if isinstance(sib_row, list)==False or len(sib_row)==0:
                    sib2.loc[sib] = [name] + row
                    # changes+=1  # added 149 
                else:
                    name_row = [name] + row
                    if sib in name_row:
                        name_row.remove(sib)                    
                    for el in name_row:
                        if el not in sib_row:
                            sib2.loc[sib].append(el)
                            # changes2+=1 #added 152 
    return sib2

sib2 = add_siblings(sib2, ids_short)
# sib2 = add_siblings(sib2, ids_short) # twice, to have all added

                
# # Total siblings in siblings column:
# tot = 0
# for i, row in enumerate(sib2.tolist()):
#         if isinstance(row, list) and len(row)!=0:
#             tot+=len(row)  # 110294


# In each row, order siblings and children alphabetically (easier to make further checks):
def order_sib_children(col, ids_short):
    col2 = []
    for i, row in enumerate(col.tolist()):
        if isinstance(row, list) and len(row)!=0:
            col2.append(sorted(row))
        else:
            col2.append(np.nan)
    return pd.Series(col2, index = ids_short)
    
sib2 = order_sib_children(sib2, ids_short)
children2 = order_sib_children(children2, ids_short)

# ------------------------

# B) Check if all siblings have same parent:

# - First, collect all different sets of siblings (to avoid checking each set multiple times):
a = sib2.dropna()
for name in a.index.tolist():
    a.loc[name].append(name)
a = order_sib_children(a, a.index.tolist())
a = a.sort_values().values.tolist() # only 4507 memes have siblings

prev_row = ['__']
sib_sets = [] # 718 sets of siblings (ie on average 4507/718=6.2 siblings in set)
for row in a:
    if row!=prev_row:
        sib_sets.append(row)
        prev_row = row
        
# Now, check if all siblings in same sibling set have same parent:  
sib_parents = []
# sib_parents_counts = []
for sib_set in sib_sets:
    row_parents = []
    for sib in sib_set:
        row_parents.append(parent2.loc[sib])
    # sib_parents_counts.append(len(list(set(row_parents))))
    sib_parents.append(list(set(row_parents))[0])
    
# sum(sib_parents_counts)==len(sib_sets) # True -> all have same parent

# ------------------------

# - For each parent, check if the parent has this meme as child:

parents3 = parent2.values.tolist()
# counts = 0
for i, name in enumerate(ids_short):
    parent = parents3[i]
    if isinstance(parent, str):
        parents_children = children2.loc[parent] 
        if isinstance(parents_children, list)==False: # only one meme
            children2.loc[parent] = [name]
            # sib2.loc[name]  # nan -> the meme had no siblings
            # counts+=1
        elif name not in parents_children: # no such memes
            children2.loc[parent].append(name)
            # counts+=1
       
# -> There was only one such meme, with no siblings - so just added the meme as its parents child. 

# ------------------------

# - For each of the children, check if they have this meme as parent;

# children3 = children2.values.tolist()

# counts, counts2 = 0, 0
# for i, name in enumerate(ids_short):
#     childs = children3[i]
#     if isinstance(childs, list):
#         for child in childs:
#             childs_parent = parent2.loc[child]
#             if isinstance(childs_parent, str)==False: # 
#                 counts+=1
#             elif childs_parent!=name:
#                 counts2+=1
              
# -> All children of each meme had this meme as parent. 


# -----------------------

# Save parents/siblings/children as relations table:

# df['parent'] = parent2
# df['siblings'] = sib2
# df['children'] = children2   

relations_df = pd.concat([parent2, children2, sib2], axis=1)
relations_df.columns = ['parent', 'children', 'siblings']
relations_df = relations_df.dropna(axis=0, how = 'all')

a = pd.Series(df.index)
b = a.isin(relations_df.index.tolist())
df['has_relations'] = b.tolist()
df = df.drop(columns=['parent', 'siblings', 'children'])


# TODO: (IF NEEDED): If any row is deleted, the deleted meme should also be removed from the columns of parents/siblings/children. 

# -----------------------------------------------

# REORDER COLUMNS

df = df[['title', 'url', 'description', 'added', 'updated', 'year', 'status', 'search_keywords', 'type', 'similar_images', 'has_relations', 'has_other_texts', 'has_refs', 'template_image_url', 'fb_image_url']]
  
# -----------------------------------------------


# SAVE ALL:

df.to_csv(path_to_cleaned + 'main_df.csv', sep=';', index=True, encoding='utf-8') 
refs_df.to_csv(path_to_cleaned + 'refs_df.csv', sep=';', index=True, encoding='utf-8') 
relations_df.to_csv(path_to_cleaned + 'relations_df.csv', sep=';', index=True, encoding='utf-8') 
textual_df.to_csv(path_to_cleaned + 'textual_df.csv', sep=';', index=True, encoding='utf-8') 


# -----------------------------------------

# # Was used for web scraping:
id_df = df[["url"]] 
id_df.to_excel(path_to_cleaned + "id_df.xlsx")




