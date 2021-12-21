import json
import sys
import pandas as pd

df = pd.read_csv("./data/processed/product_en_sex.csv")

# https://pretagteam.com/question/convert-csv-data-to-nested-json-in-python

records = []

with open("template.json") as infile:
    j = json.load(infile)

for key, grp in df.groupby(['GEO']):
   j["dimension"][0]["member"].append({
      "memberId": "1",
      "memberNameEn": key,
      "memberNameFr": ".",
      "memberUomCode": "."
   })

with open("./data/processed/metadata.json", "w") as outfile:
    json.dump(j, outfile, indent=4)