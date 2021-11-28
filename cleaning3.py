# Data 3 - kym_spotlight.json
# Packages
import numpy as np
import pandas as pd
import json 

# Original cleaned data url for joining 
id_df = pd.read_excel('id_df.xlsx', index_col=0)  

# Loading data
with open('kym_spotlight.json', encoding='utf-8') as fs:
    data3 = json.load(fs)

df_ = pd.DataFrame(data3)
df_2=df_.T # 7603 x 7

# Columns: 7, only meaningful one seems 'Resources'
s2 = df_2['Resources'].apply(pd.Series).add_prefix('Resources.')
# 53 stacked columns, extracting 3 for example
s3 = s2['Resources.0'].apply(pd.Series).add_prefix('Resources.0')
s4 = s2['Resources.1'].apply(pd.Series).add_prefix('Resources.1')
s5 = s2['Resources.2'].apply(pd.Series).add_prefix('Resources.2')

# Saving 2 examples
s = pd.concat([s3, s4], axis=1) 

resources_df = id_df.merge(s, left_on='url', right_index=True)

resources_df.to_excel("resources_df.xlsx") 