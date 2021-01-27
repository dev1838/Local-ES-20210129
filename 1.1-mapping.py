from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

es = Elasticsearch("https://464d6c48623b49a18b4d04d8dd301996.asia-east1.gcp.elastic-cloud.com:9243", http_auth=('elastic', 'q4dT1Cht7K8aSGOIlnPMDae1'))

this_index = 'price_sma'

# mapping
index_body = {
    "settings": {
        "index": { "number_of_shards": 1,  "number_of_replicas": 1 }
    },
    "mappings": {
        "properties": {
            "Stock_id" : {"type" : "keyword"},
            "Date" : {"type" : "date", "format": "yyyy-MM-dd"},
            "close_sma_20" : {"type" : "float"},
            "close_sma_60" : {"type" : "float"},
            "close_sma_100" : {"type" : "float"},
            "volume_sma_5" : {"type" : "float"},
            "volume_sma_20" : {"type" : "float"},
        }
    }
}

result = es.indices.create(index='price_sma', body=index_body)