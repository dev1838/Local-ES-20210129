import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

es = Elasticsearch("https://464d6c48623b49a18b4d04d8dd301996.asia-east1.gcp.elastic-cloud.com:9243", http_auth=('elastic', 'q4dT1Cht7K8aSGOIlnPMDae1'))

index_name = "price_sma"

s = Search(using=es, index=index_name) \
    .filter("range", Date={"gte": "now-10000d/d","lt": "now/d"}) \
    .query("match", Stock_id=700) \
    .sort({"Date": {"order": "desc"}})    

s = s[0:20]

response = s.execute()
elastic_docs = response["hits"]["hits"]

doc_fields = {}
for num, doc in enumerate(elastic_docs):
    source_data = doc["_source"]
    for key in source_data:
        try:
            doc_fields[key] = np.append(doc_fields[key], source_data[key])
        except KeyError:
            doc_fields[key] = np.array([source_data[key]])

# for key, val in doc_fields.items():
#     print (key, ":", val) # numpy array

elastic_df = pd.DataFrame(doc_fields)

print ('elastic_df:', type(elastic_df), "\n")
print (elastic_df) # print out the DF object's contents