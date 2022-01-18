import json
import sys
import pandas as pd
import numpy as np
import ast

df = pd.read_csv("./data/processed/product_en_sex.csv")

with open("template.json") as infile:
    j = json.load(infile)

grouped = df.groupby(['GEO','COORDINATE']).count().reset_index().drop_duplicates(subset=['GEO'])

grouped_members = df.groupby(['Member']).count().reset_index()

grouped_sex = df.groupby(['Sex']).count().reset_index()

grouped['row'] = range(1, grouped.shape[0] + 1) 

grouped_members['row'] = range(1, grouped_members.shape[0] + 1) 

grouped_sex['row'] = range(1, grouped_sex.shape[0] + 1) 

# for index, row in grouped.iterrows():
#    print(row['GEO'])

for index, row in grouped.iterrows():
   j["dimension"][0]["member"].append({
      # "memberId": json.dumps(list(grp['row']), sort_keys=True, indent=4),
      "memberId": str(row['COORDINATE']).split(".")[0],
      "memberNameEn": row['GEO'],
      "memberNameFr": ".",
      "memberUomCode": "."
   })

for index, row in grouped_members.iterrows():
   j["dimension"][1]["member"].append({
      "memberId": row['row'],
      "memberNameEn": row['Member'],
      "memberNameFr": ".",
      "memberUomCode": "."
   })

for index, row in grouped_sex.iterrows():
   j["dimension"][2]["member"].append({
      "memberId": row['row'],
      "memberNameEn": row['Sex'],
      "memberNameFr": ".",
      "memberUomCode": "."
   })

# for key, grp in df.groupby(['GEO']):
#    j["dimension"][0]["member"].append({
#       "memberId": ".",
#       "memberNameEn": key,
#       "memberNameFr": ".",
#       "memberUomCode": "."
#    })

# for key, grp in df.groupby(['Member']):
#    j["dimension"][1]["member"].append({
#       "memberId": ".",
#       "memberNameEn": key,
#       "memberNameFr": ".",
#       "memberUomCode": "."
#    })

# for key, grp in df.groupby(['Sex']):
#    j["dimension"][2]["member"].append({
#       "memberId": ".",
#       "memberNameEn": key,
#       "memberNameFr": ".",
#       "memberUomCode": "."
#    })

with open("./data/processed/metadata.json", "w") as outfile:
    json.dump(j, outfile, indent=4)