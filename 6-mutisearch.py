from elasticsearch import Elasticsearch
from elasticsearch_dsl import MultiSearch, Search

import numpy as np
import pandas as pd

es = Elasticsearch("https://464d6c48623b49a18b4d04d8dd301996.asia-east1.gcp.elastic-cloud.com:9243", http_auth=('elastic', 'q4dT1Cht7K8aSGOIlnPMDae1'))

index_name = "price_csv"

stock_id_list = [500, 700]
ms_daily = MultiSearch(using=es)
for stock_id in stock_id_list:    
    ms_daily = ms_daily.add(Search(index=index_name).\
        query("match", Stock_id=stock_id).\
        source(includes=['Stock_id', 'Date', 'Close', 'Volume']).\
        sort({"Date": {"order": "desc"}}))

responses_daily = ms_daily.execute()
doc_fields_daily = {}
for response in responses_daily:
    source_data = response["hits"]["hits"][0]["_source"]
    for key in source_data:
        try:
            doc_fields_daily[key] = np.append(doc_fields_daily[key], source_data[key])
        except KeyError:
            doc_fields_daily[key] = np.array([source_data[key]])

elastic_df_daily = pd.DataFrame(doc_fields_daily)
print(elastic_df_daily)