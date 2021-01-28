from elasticsearch import Elasticsearch
from elasticsearch_dsl import MultiSearch, Search

import numpy as np
import pandas as pd

es = Elasticsearch("https://464d6c48623b49a18b4d04d8dd301996.asia-east1.gcp.elastic-cloud.com:9243", http_auth=('elastic', 'q4dT1Cht7K8aSGOIlnPMDae1'))

index_daily = "price_csv"
index_sma = "price_sma"

# Step 1: 搜尋最新的盤後資訊與移動平均資訊
ms_daily = MultiSearch(using=es)
ms_sma = MultiSearch(using=es)

stock_id_list = [500, 700]
for stock_id in stock_id_list: 
    search_daily = Search(index=index_daily).\
        query("match", Stock_id=stock_id).\
        source(includes=['Stock_id', 'Date', 'Close', 'Volume']).\
        sort({"Date": {"order": "desc"}})
    search_daily = search_daily[0:1]    
    ms_daily = ms_daily.add(search_daily)

    search_sma = Search(index=index_sma).\
        query("match", Stock_id=stock_id).\
        source(includes=['Stock_id', 'Date', 'close_sma_100', 'volume_sma_20']).\
        sort({"Date": {"order": "desc"}})
    search_sma = search_sma[0:1]    
    ms_sma = ms_sma.add(search_sma)
    
responses_daily = ms_daily.execute()
responses_sma = ms_sma.execute()
print(responses_daily)

# Step 2: 將搜尋結果轉換成 Pandas.Dataframe
def response_to_dataframe(es_multi_responses):
    doc_fields = {}
    count = 0
    for response in es_multi_responses:
        count += 1
        if not bool(response):
            continue
        source_data = response["hits"]["hits"][0]["_source"]
        for key in source_data:
            try:
                doc_fields[key] = np.append(doc_fields[key], source_data[key])
            except KeyError:
                doc_fields[key] = np.array([source_data[key]])
    return pd.DataFrame(doc_fields)

elastic_df_daily = response_to_dataframe(responses_daily)
elastic_df_sma = response_to_dataframe(responses_sma)

print(elastic_df_daily)
print(elastic_df_sma)

elastic_df_merge = pd.merge(elastic_df_daily, elastic_df_sma, on=['Stock_id','Date'])
#print(elastic_df_merge)

# Step 3: 利用 Pandas 進行選股
elastic_df_merge['Qualify'] = np.where(
(elastic_df_merge['Volume'].astype(np.float) >= 1.5 * elastic_df_merge['volume_sma_20'].astype(np.float)) &
(elastic_df_merge['Close'].astype(np.float) >=elastic_df_merge['close_sma_100'].astype(np.float)), True, False)
                  
df_select = elastic_df_merge.loc[elastic_df_merge['Qualify'] == True]
print(df_select)