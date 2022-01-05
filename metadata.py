import json
import sys
import pandas as pd
import numpy as np
import ast

df = pd.read_csv("./data/processed/product_en_sex.csv")

with open("template.json") as infile:
    j = json.load(infile)

grouped = df.groupby(['GEO']).count().reset_index()

grouped['row'] = range(1, grouped.shape[0] + 1) 

# for index, row in grouped.iterrows():
#    print(row['GEO'])

for index, row in grouped.iterrows():
   j["dimension"][0]["member"].append({
      # "memberId": json.dumps(list(grp['row']), sort_keys=True, indent=4),
      "memberId": row['row'],
      "memberNameEn": row['GEO'],
      "memberNameFr": ".",
      "memberUomCode": "."
   })

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