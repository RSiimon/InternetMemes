# Packages
import numpy as np
import pandas as pd
import json 

# Loading data
with open('kym.json') as f:
    data = json.load(f)

df = pd.DataFrame(data)

print(df.head())
sorted(df)

# Selecting only memes
df2 = df.set_index(['category'])
df3 = df2.loc['Meme'].reset_index()

# Dropping unneccessary columns
df4 = df3.drop(columns=['ld', 'meta', 'details', 'content', 'additional_references'])

# Dropping duplicates
df5 = df4.drop_duplicates(subset=['title', 'url'])

# Dropping memes with bad-words and sensitive content
drop_values = ['fuck', 'shit', 'dick', 'Fuck', 'Shit', 'Dick', 'FUCK', 'penis', 'Penis', 'PENIS', 'Gay', 'gay', 'GAY',
'Naked', 'poop', 'Poop', 'Nigga', 'Nigger', 'nigga', 'Suck', 'Cock', 'Porn', 'porn', 'Ass', 'Raped', 'raped', 'Rape',
'Pussy', 'Cunt', 'Bitch']
df6 = df5[~df5['title'].str.contains('|'.join(drop_values))]

# Writing to Excel
df6.to_excel("df.xlsx")



