# -*- coding: utf-8 -*-

# Data 3 - kym_spotlight.json

import numpy as np
import pandas as pd
import string

fname = "kym_spotlight.json"
fname_main_df = 'main_df.csv'
fname_refs = 'refs_df.csv'
fname_rel = 'relations_df.csv'
fname_textual = 'textual_df.csv'
fname_vision = 'vision_annot_df.csv'


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


def cleaning_3():
    # Loading data
    df = pd.read_json(fname, orient='index')
    df_main = pd.read_csv(fname_main_df, sep=';', index_col=0, encoding='utf-8')
    refs_df = pd.read_csv(fname_refs, sep=';', index_col=0, encoding='utf-8')
    relations_df = pd.read_csv(fname_rel, sep=';', index_col=0, encoding='utf-8')
    textual_df = pd.read_csv(fname_textual, sep=';', index_col=0, encoding='utf-8')
    vision_annot_rev_df = pd.read_csv(fname_vision, sep=';', index_col=0, encoding='utf-8')

    df_main = convert_list_col('similar_images', df_main)
    df_main = convert_list_col('similar_images2', df_main)
    df_main = convert_list_col('image_recogn_labels', df_main)
    df_main = convert_list_col('image_recogn_scores', df_main)
    df_main = convert_list_col('alt_img_urls', df_main)
    df_main = convert_list_col('search_keywords', df_main)
    df_main = convert_list_col('type', df_main)
    df_main = convert_list_col('type2', df_main)
    relations_df = convert_list_col('siblings', relations_df)
    relations_df = convert_list_col('children', relations_df)
    vision_annot_rev_df = convert_list_col('memes', vision_annot_rev_df)
    vision_annot_rev_df = convert_list_col('scores', vision_annot_rev_df)
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

    # len(df) # 7003

    # -----------------------------------------------

    # DELETE COLUMNS CONTAINING A SINGLE VALUE:
    # len(set(df['@confidence'].tolist())) # single value
    # len(set(df['@support'].tolist())) # single value
    # len(set(df['@types'].tolist())) # single value
    # len(set(df['@sparql'].tolist())) # single value
    # len(set(df['@policy'].tolist())) # single value

    df = df.drop(columns=['@confidence', '@support', '@types', '@sparql', '@policy'])  # -> Only 2 columns were left.

    # -----------------------------------------------

    # @TEXT COLUMN:

    # - Replace values in 'description' column with the ones in '@text' column, leaving the old descriptions in place in rows where '@text' is empty.

    # ------------

    # COMPARE @TEXT WITH 'DESCRIPTION' COLUMN:

    # # # Check if '@text' contains same info as 'description' in main table:
    # a = df_main['description']
    # # a.index = a['url']
    # # df.index = df['url']
    # joined = pd.concat([a, df['@text']], sort = False, axis=1)
    # joined['has_desc'] = joined['@text'].isna().values==False
    # joined = joined[joined['has_desc']==True]

    # joined['same'] = joined['description'] == joined['@text']
    # np.sum(joined['same']) # only 6 descriptions are exactly the same
    # # At the same time, they often seemed to have only minor differences.
    # # -> compare differences for the rest:

    # joined = joined[joined['same']==False]
    # joined = joined.drop(columns = ['same', 'has_desc'])

    # # For examining differences:
    # texts = joined['@text'][:300].tolist()
    # descs = joined['description'][:300].tolist()
    # # -> Quality of descriptions in '@text' column seems slightly better.

    # ------------

    # Merge the columns:

    for i, text in enumerate(df['@text']):
        if isinstance(text, str) == True and len(text) != 0:
            name = df.index.tolist()[i]
            df_main.loc[name]['description'] = text

    # -----------------------------------------------

    # 'RESOURCES' COLUMN:

    # m = df['url'].tolist()
    res = df['Resources'].tolist()

    # - This column contains links to DBpedia on keywords found in '@text' column. Each row is a list of dictionaries, with each dictionary containing the link, as well as keyword, offset (referring to keyword's location in text), similarity score and support, and  @percentageOfSecondRank (which "measures by how much the winning entity has won, calculated as contextualScore_2ndRank / contextualScore_1stRank; the lower the value, the further the first ranked entity was "in the lead").

    # - Add keywords to "search_keywords" in main table.
    # - Add keywords with similarity scores and links to DBPedia entries as separate table.
    # - Add column "has_DBpedia" to main table.
    # Don't include non-informative keywords (identified those by examining 300 most frequent keywords)

    excluded = ['meme', 'YouTube', 'image macro', 'catchphrase', 'macros', 'Twitter', 'expression', '4chan', 'Tumblr',
                'hashtag', 'remix', 'Internet', 'Reddit', 'social media', 'slang', 'English', 'Facebook', 'viral video',
                'video sharing', 'fandom', 'copypasta', 'music video', 'phrasal template', 'Instagram', 'YTMND',
                'YouTube Poop', 'GIF', 'Meme', 'imageboard', 'Internet slang', 'internet slang', 'YouTuber', 'Google',
                'subreddit', 'Photoshop', 'MS Paint', 'Go', 'This Is', '/pol/', 'animated GIF', 'flash animation',
                'internet meme', 'macro', 'internet memes', 'Youtube Poop', '2channel', 'fuck', 'stock photo', '3D',
                'Windows', 'verb', '2chan', 'signify', 'GIF animation', 'Internet slang', 'internet slang']

    keywords2, scores2, links2 = [], [], []

    for row in res:
        if isinstance(row, list) == True and len(row) != 0:
            keywords, scores, links = [], [], []
            for el in row:
                keyword = el['@surfaceForm']
                if keyword not in keywords and keyword not in excluded:  # exclude duplicates and non-informative keywords
                    scores.append(el['@similarityScore'])
                    keywords.append(keyword)
                    links.append(el['@URI'])

            if len(keywords) > 0:
                # sort by similarity scores:
                d = pd.DataFrame(data=[keywords, scores, links]).T
                d = d.sort_values(by=[1], ascending=False)

                scores2.append(d[1].tolist())
                keywords2.append(d[0].tolist())
                links2.append(d[2].tolist())
            else:
                scores2.append(np.nan)
                keywords2.append(np.nan)
                links2.append(np.nan)
        else:
            scores2.append(np.nan)
            keywords2.append(np.nan)
            links2.append(np.nan)

        # 10392 different keywords

    # # IDENTIFY KEYWORDS TO BE EXCLUDED:
    # a = pd.DataFrame(data = [keywords2, scores2, links2]).T
    # all_words = []
    # for row in a[0].tolist():
    #     if isinstance(row, list)==True and len(row)!=0:
    #         all_words+=row

    # all_words = pd.Series(all_words).value_counts()
    # for w in all_words.index.tolist()[:300]:
    #     print("'" + w+ "'", end = ", ")

    DBpedia_df = pd.DataFrame(data=[keywords2, scores2, links2]).T
    DBpedia_df.index = df.index.tolist()
    DBpedia_df = DBpedia_df.dropna(axis=0, how='all')
    DBpedia_df.columns = ['keywords', 'scores', 'links']
    df_main['db_keywords'] = DBpedia_df['keywords']

    df_main["has_DBpedia"] = df_main.index.isin(DBpedia_df.index)

    # ---------------------------------------------

    # MERGE ALL KEYWORD COLUMNS ('type', 'type2', 'db_keywords', 'search keywords'):

    def merge_types(col1, col2, df_main):
        typesA = df_main[col1].tolist()
        typesB = df_main[col2].tolist()
        types2 = []
        for i, row in enumerate(typesA):
            if isinstance(row, list) == False or len(row) == 0:
                types2.append(typesB[i])
            elif isinstance(typesB[i], list) == False or len(typesB[i]) == 0:
                types2.append(row)
            else:
                types2.append(row + typesB[i])

        df_main[col1] = types2
        df_main = df_main.drop(columns=[col2])

        return df_main

    df_main = merge_types('type', 'type2', df_main)
    df_main = merge_types('type', 'db_keywords', df_main)
    df_main = merge_types('search_keywords', 'type', df_main)

    # ---------------------------

    # - WRANGLING KEYWORDS:

    # - lowercase keywords,
    # - replace phrases with single words,
    # - drop duplicate keywords of each meme
    # - exclude keywords with length 1.
    # - remove punctuation
    # - exclude numbers

    keywords = []
    for row in df_main['search_keywords'].tolist():
        if isinstance(row, list) == True and len(row) != 0:
            words2 = []
            for phrase in row:
                words = phrase.split(' ')
                for word in words:
                    word = word.strip().lower()
                    for c in string.punctuation:
                        word = word.replace(c, "")
                    for d in string.digits:
                        word = word.replace(d, "")
                    if len(word) > 1:
                        words2.append(word)
            if len(words2) > 0:
                keywords.append(sorted(list(set(words2))))
            else:
                keywords.append(np.nan)
        else:
            keywords.append(np.nan)

    df_main['search_keywords'] = keywords

    # Remove non-informative keyowrds:
    # - Reviewed 500 most frequent words and excluded those that seemed irrelevant:

    # # Value counts of all words:
    # all_words = []
    # for row in keywords:
    #     all_words+=row
    # all_words = pd.Series(all_wor2ds).value_counts()
    # for a in all_words[:500].index:
    #     print("'" + a + "'", end = ', ')

    # 37271 keywords

    stopwords = ['meme', 'image', 'internet', 'your', 'know', 'the', 'of', 'gif', 'video', 'and', 'photo', 'clip',
                 'font',
                 'caption', 'catchphrase', 'text', 'blog', 'macro', 'you', 'is', 'my', 'in', 'media', 'memes', 'to',
                 'youtube', 'on', 'for', 'me', 'it', 'with', 'jpeg', 'this', 'online', 'wiki', 'one', 'that', 'not',
                 'at',
                 'can', 'are', 'what', 'download', 'up', 'youtuber', 'do', 'its', 'all', 'dont', 'us', 'we', 'get',
                 'have',
                 'has', 'twitter', 'be', 'ii', 'from', 'am', 'will', 'de', 'out', 'how', 'facebook', 'pictures', 'just',
                 'an', 'iii', 'was', 'if', 'who', 'when', 'over', 'picture', 'by', 'down', 'cant']

    keywords = []
    for row in df_main['search_keywords'].tolist():
        if isinstance(row, list) == True and len(row) != 0:
            words2 = []
            for word in row:
                if word not in stopwords:
                    words2.append(word)
            if len(words2) > 0:
                keywords.append(sorted(list(set(words2))))
            else:
                keywords.append(np.nan)
        else:
            keywords.append(np.nan)

    df_main['search_keywords'] = keywords

    # df_main['search_keywords'].isna().sum()  # 0 - no missing values

    # -----------------------------------------------

    # REORDER COLUMNS

    df_main = df_main[
        ['title', 'url', 'description', 'added', 'updated', 'year', 'has_relations', 'has_other_texts', 'has_refs',
         'has_DBpedia', 'status', 'adult', 'medical', 'racy', 'spoof', 'violence', 'template_image_url', 'fb_image_url',
         'alt_img_urls', 'similar_images', 'similar_images2', 'image_recogn_labels', 'image_recogn_scores',
         'search_keywords']]

    # -----------------------------------------------

    # # OVERVIEW OF ALL COLUMNS:   (dtypes and missing values)

    # # Missing values and types:
    # def make_cols_overview(df2):
    #     missing, types = [], []
    #     for col in df2.columns.tolist():
    #         types.append(str(type(df2[col].iloc[0])).split("'")[1])
    #         missing.append(df2[col].isna().sum())

    #     counts_df = pd.DataFrame(data=[df2.columns.tolist(), types, missing]).T
    #     counts_df.columns = ['column', 'type', 'missing']
    #     print(len(df2), 'rows, ', len(df2.columns.tolist()), 'columns')

    #     return counts_df

    # counts_main = make_cols_overview(df_main) # 11610 rows,  24 columns
    # counts_refs = make_cols_overview(refs_df) # 3655 rows,  7 columns
    # counts_relations = make_cols_overview(relations_df) # 4672 rows,  3 columns
    # counts_textual = make_cols_overview(textual_df) # 6935 rows,  3 columns
    # counts_vision = make_cols_overview(vision_annot_rev_df) # 4410 rows,  2 columns
    # counts_DBpedia = make_cols_overview(DBpedia_df) # 6539 rows,  3 columns

    # -----------------------------------------------

    # SAVE ALL:

    # TEMPORARY TABLES CONTAINING LISTS (MAYBE BETTER TO GET OVERVIEW):

    df_main.to_csv('main_df.csv', sep=';', index=True, encoding='utf-8')
    refs_df.to_csv('refs_df.csv', sep=';', index=True, encoding='utf-8')
    relations_df.to_csv('relations_df.csv', sep=';', index=True, encoding='utf-8')
    textual_df.to_csv('textual_df.csv', sep=';', index=True, encoding='utf-8')
    vision_annot_rev_df.to_csv('vision_annot_df.csv', sep=';', index=True, encoding='utf-8')
    DBpedia_df.to_csv('DBpedia_df.csv', sep=';', index=True, encoding='utf-8')

    # -----------------------------------------------

    # CONVERT COLUMNS CONTAINING LISTS TO JSON, SAVE FINAL TABLES AND JSONS:

    # CONVERSIONS:
    # df_main -> several jsons, and df
    # refs_df -> only df
    # textual_df -> only df
    # relations_df -> only json
    # vision_annot_rev_df  -> only json
    # DBpedia_df -> only json

    # relations_df:  (lists: 'children', 'siblings', str: parent)
    relations_df.to_json('relations.json', force_ascii=False, orient='index')  # orient = 'columns'
    # {meme_id: {'parent': 'shock-sites', 'children': [..], 'siblings': [...]},
    # ... }

    # DBpedia_df:  'keywords', 'scores', 'links' (all lists)
    DBpedia_df.to_json('DBpedia.json', force_ascii=False, orient='index')
    # {meme_id: {'keywords': [...], 'scores': [...], 'links': [...]},
    # ... }

    # vision_annot_rev_df:  'memes', 'scores' (all lists)
    vision_annot_rev_df.to_json('vision_annot.json', force_ascii=False, orient='index')
    # {keyword: {'memes': [meme_id1, meme_id2, ...], 'scores': [...]},
    # ... }

    # df_main:
    #  'alt_img_urls', 'similar_images', 'similar_images2', 'image_recogn_labels', 'image_recogn_scores', 'search_keywords'
    df_similar_img_urls = df_main[['similar_images', 'similar_images2']]
    df_similar_img_urls = df_similar_img_urls.dropna(axis=0, how='all')
    df_similar_img_urls.to_json('similar_img_urls.json', force_ascii=False, orient='index')
    # {meme_id: {'similar_images': [...], 'similar_images2': [...]},
    # ... }

    df_image_recogn = df_main[['image_recogn_labels', 'image_recogn_scores']]
    df_image_recogn.to_json('image_recogn.json', force_ascii=False, orient='index')
    # {meme_id: {'image_recogn_labels': [...], 'image_recogn_scores': [...]},
    # ... }

    df_alternative_urls = df_main[['alt_img_urls']]  # missing values
    df_alternative_urls = df_alternative_urls.dropna(axis=0, how='all')
    df_alternative_urls.to_json('alternative_urls.json', force_ascii=False,
                                orient='columns')  # 'index'

    df_search = df_main[['search_keywords']]
    df_search.to_json('search_keywords.json', force_ascii=False, orient='columns')

    # No lists in: refs_df, textual_df:
    textual_df.to_csv('textual_df.csv', sep=';', index=True, encoding='utf-8')
    refs_df.to_csv('refs_df.csv', sep=';', index=True, encoding='utf-8')

    # df main, without lists:
    df_main = df_main.drop(
        columns=['alt_img_urls', 'similar_images', 'similar_images2', 'image_recogn_labels', 'image_recogn_scores',
                 'search_keywords'])
    df_main.to_csv('main_df.csv', sep=';', index=True, encoding='utf-8')