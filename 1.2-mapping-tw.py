from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

es = Elasticsearch("https://464d6c48623b49a18b4d04d8dd301996.asia-east1.gcp.elastic-cloud.com:9243", http_auth=('elastic', 'q4dT1Cht7K8aSGOIlnPMDae1'))

index_name = "price_tw"

index_body = {
    "settings": {
        "index": { "number_of_shards": 1,  "number_of_replicas": 1 }
    },
    "mappings": {
        "properties": {
            "證券代號," : {"type" : "keyword"},
            "證券名稱" : {"type": "keyword"},  #some words con't load?
            "Date" : {"type" : "date", "format" : "yyyy-MM-dd"},
            "成交股數" : {"type" : "long"},
            "成交金額" : {"type" : "long"},
            "開盤價" : {"type" : "float"},
            "最高價" : {"type" : "float"},
            "最低價" : {"type" : "float"},
            "收盤價" : {"type" : "float"},
            "漲跌價差" : {"type" : "float"},
            "成交筆數" : {"type" : "integer"}
        }
  }
}


result = es.indices.create(index=index_name, body=index_body)