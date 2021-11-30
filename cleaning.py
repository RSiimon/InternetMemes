# Data 1 - kym.json
# Packages
import numpy as np
import pandas as pd
import json 

# Loading data
with open('kym.json') as f:
    data = json.load(f)

df = pd.DataFrame(data)

# Selecting only memes
df2 = df.set_index(['category'])
df3 = df2.loc['Meme'].reset_index()

# Dropping unneccessary columns
df4 = df3.drop(columns=['category'])

# Dropping duplicates
df5 = df4.drop_duplicates(subset=['title', 'url']) # korras df - v.a jargmised kolumnid 

# Dropping memes with bad-words and sensitive content
drop_values = ['fuck', 'shit', 'dick', 'Fuck', 'Shit', 'Dick', 'FUCK', 'penis', 'Penis', 'PENIS', 'Gay', 'gay', 'GAY',
'Naked', 'poop', 'Poop', 'Nigga', 'Nigger', 'nigga', 'Suck', 'Cock', 'Porn', 'porn', 'Ass', 'Raped', 'raped', 'Rape',
'Pussy', 'Cunt', 'Bitch']
df6 = df5[~df5['title'].str.contains('|'.join(drop_values))].reset_index()

# Saving id columns and base df for joining
id_df = df6[["url"]]
df = df6.drop(columns=['ld', 'meta', 'details', 'content', 'additional_references'])

id_df.to_excel("id_df.xlsx")
df.to_excel("df.xlsx")

# Extracting stacked columns ('ld', 'meta', 'details', 'content', 'additional_references') 
# and saving them to dataframes, Excel
s1 = df6['meta'].apply(pd.Series) 
s1_2 = pd.concat([id_df, s1], axis=1)
s1_2.to_excel("meta_df.xlsx")

#s2 = df5['ld'].apply(pd.Series).add_prefix('ld.') # unneccessay 
#s2_2 = s2['ld.itemListElement'].apply(pd.Series).add_prefix('ld.itemListElement')
#s2_3 = s2_2['ld.itemListElement0'].apply(pd.Series).add_prefix('ld.itemListElement0')
#s2_4 = s2_3['ld.itemListElement0position'].apply(pd.Series).add_prefix('ld.itemListElement0position')

s3 = df6['details'].apply(pd.Series) 
s3_2 = pd.concat([id_df, s3], axis=1) 
s3_2.to_excel("details_df.xlsx")

s4 = df6['content'].apply(pd.Series).add_prefix('content.') # 2400+ stacked columns, mostly 1-2 memes available for each column
s4_2 = s4['content.about'].apply(pd.Series).add_prefix('about.')
s4_3 = s4['content.origin'].apply(pd.Series).add_prefix('origin.') 
s4_4 = s4['content.spread'].apply(pd.Series).add_prefix('spread.')
s4_5 = s4['content.search interest'].apply(pd.Series).add_prefix('search interest.') 
s4_6 = s4['content.external references'].apply(pd.Series).add_prefix('external references.')

df_about = s4_2[['about.images', 'about.links', 'about.text']]
df_origin = s4_3[['origin.images', 'origin.links', 'origin.text']]
df_spread = s4_4[['spread.images', 'spread.links', 'spread.text']]
df_si = s4_5[['search interest.text', 'search interest.links']]
df_er = s4_6[['external references.text', 'external references.links']]
df_s4 = pd.concat([id_df, df_about, df_origin, df_spread, df_si, df_er], axis=1)

df_s4.to_excel("content_df.xlsx")

s5 = df6['additional_references'].apply(pd.Series) # 100+ sites of reference, mostly 1-2 memes for each, selecting sites with more memes
s5_2 = s5[["Twitter,", "Reddit", "Reddit,", "Meme Generator", "Encyclopedia Dramatica", "Urban Dictionary",
"Meme Generator,", "Wikipedia", "Encyclopedia Dramatica,", "Twitter"]]
s5_2['Twitter']= s5_2['Twitter'].combine_first(s5_2['Twitter,'])
s5_2['Reddit']=s5_2['Reddit'].combine_first(s5_2['Reddit,'])
s5_2['Encyclopedia Dramatica']=s5_2['Encyclopedia Dramatica'].combine_first(s5_2['Encyclopedia Dramatica,'])
s5_2['Meme Generator']=s5_2['Meme Generator'].combine_first(s5_2['Meme Generator,'])
s5_3 = s5_2.drop(columns=['Twitter,','Reddit,','Encyclopedia Dramatica,','Meme Generator,'])
s5_3 = pd.concat([id_df, s5_3], axis=1)

s5_3.to_excel("additional_references_df.xlsx")
