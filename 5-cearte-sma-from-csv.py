from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

import numpy as np
import pandas as pd

import talib

es = Elasticsearch("https://464d6c48623b49a18b4d04d8dd301996.asia-east1.gcp.elastic-cloud.com:9243", http_auth=('elastic', 'q4dT1Cht7K8aSGOIlnPMDae1'))

index_ref = "price_csv"
index_name = "price_sma"

s = Search(using=es, index=index_ref) \
    .filter("range", Date={"gte": "now-10000d/d","lt": "now/d"}) \
    .query("match", Stock_id=500) \
    .sort({"Date": {"order": "desc"}})    

s = s[0:240]

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

elastic_df = pd.DataFrame(doc_fields)

## DF
print ('elastic_df:', type(elastic_df), "\n")
print (elastic_df) # print out the DF object's contents

## SMA
closePrices = elastic_df.iloc[:, 6].astype('float').values
close_sma_20 = np.round(talib.SMA(closePrices, timeperiod=20), 2)

closePrices = elastic_df.iloc[:, 6].astype('float').values
close_sma_60 = np.round(talib.SMA(closePrices, timeperiod=60), 2)

closePrices = elastic_df.iloc[:, 6].astype('float').values
close_sma_100 = np.round(talib.SMA(closePrices, timeperiod=100), 2)

volume = elastic_df.iloc[:, 7].astype('float').values
volume_sma_5 = np.round(talib.SMA(volume, timeperiod=5), 2)

volume = elastic_df.iloc[:, 7].astype('float').values
volume_sma_20 = np.round(talib.SMA(volume, timeperiod=20), 2)

stock_id = elastic_df.iloc[:, 0].values
date = elastic_df.iloc[:, 1].values



# put csv & sma data
stock_id = elastic_df.iloc[:, 0].values
date = elastic_df.iloc[:, 1].values

documents = []
for i in range(len(stock_id)):
    document = {}
    document['Stock_id'] = stock_id[i]
    document['Date']     = date[i]
    document['close_sma_20']   = close_sma_20[i]
    document['close_sma_60']   = close_sma_60[i]
    document['close_sma_100']  = close_sma_100[i]
    document['volume_sma_5']    = volume_sma_5[i]
    document['volume_sma_20']   = volume_sma_20[i]

    action = {}
    actionProperties = {}
    actionProperties["_id"] = document['Stock_id']  + document['Date']
    action["index"] = actionProperties
    documents.append(action)
    documents.append(document)

result = es.bulk(body=documents, index=index_name)