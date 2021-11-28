# Data 2 - kym_vision.json
# Packages
import numpy as np
import pandas as pd
import json 

# Original cleaned data title and url for joining 
id_df = pd.read_excel('id_df.xlsx', index_col=0)  

# Loading data
df = pd.read_json("C:/Users/egles/Documents/GitHub/InternetMemes/kym_vision.json",orient='index')
# stacked columns: labelAnnotations, safeSearchAnnotation, webDetection
s = df['labelAnnotations'].apply(pd.Series).add_prefix('labelAnnotations.') # stacked, unneccessary 

s1 = df['safeSearchAnnotation'].apply(pd.Series).add_prefix('safeSearchAnnotation.') # not stacked
safe_df = id_df.merge(s1, left_on='url', right_index=True)
safe_df2 = safe_df.drop(columns=['safeSearchAnnotation.0'])
safe_df2.to_excel("safesearch_df.xlsx")

s2 = df['webDetection'].apply(pd.Series).add_prefix('webDetection.') 
# 6 stacked columns: bestGuessLabels, fullMatchingImages, partialMatchingImages,
# pagesWithMatchingImages, visuallySimilarImages, webEntities
s22 = s2['webDetection.bestGuessLabels'].apply(pd.Series).add_prefix('webDetection.bestGuessLabels.')
s23 = s22['webDetection.bestGuessLabels.0'].apply(pd.Series).add_prefix('webDetection.bestGuessLabels.0.') # not stacked - guessed labels
guesslabels_df = id_df.merge(s23, left_on='url', right_index=True)
guesslabels_df2 = guesslabels_df.drop(columns=['webDetection.bestGuessLabels.0.0'])
guesslabels_df2.to_excel("guesslabels_df.xlsx")

#s33 = s2['webDetection.fullMatchingImages'].apply(pd.Series).add_prefix('webDetection.fullMatchingImages')
#s34 = s2['webDetection.pagesWithMatchingImages'].apply(pd.Series).add_prefix('webDetection.pagesWithMatchingImages')
s35 = s2['webDetection.webEntities'].apply(pd.Series).add_prefix('webEntities.')
# 15 stacked columns - unstacking 1st
s36 = s35['webEntities.0'].apply(pd.Series).add_prefix('webEntities.0.') # not stacked - score and description
webentities_df = id_df.merge(s36, left_on='url', right_index=True)
webentities_df2 = webentities_df.drop(columns=['webEntities.0.0'])
webentities_df2.to_excel("webentities_df.xlsx")
