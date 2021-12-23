import json
import sys
import pandas as pd

df = pd.read_csv("./data/processed/product_en_sex.csv")

with open("template.json") as infile:
    j = json.load(infile)

for key, grp in df.groupby(['GEO']):
   j["dimension"][0]["member"].append({
      "memberId": "1",
      "memberNameEn": key,
      "memberNameFr": ".",
      "memberUomCode": "."
   })

for key, grp in df.groupby(['Member']):
   j["dimension"][1]["member"].append({
      "memberId": ".",
      "memberNameEn": key,
      "memberNameFr": ".",
      "memberUomCode": "."
   })

for key, grp in df.groupby(['Sex']):
   j["dimension"][2]["member"].append({
      "memberId": ".",
      "memberNameEn": key,
      "memberNameFr": ".",
      "memberUomCode": "."
   })

with open("./data/processed/metadata.json", "w") as outfile:
    json.dump(j, outfile, indent=4)