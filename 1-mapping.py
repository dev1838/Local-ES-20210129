from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

es = Elasticsearch("https://464d6c48623b49a18b4d04d8dd301996.asia-east1.gcp.elastic-cloud.com:9243", http_auth=('elastic', 'q4dT1Cht7K8aSGOIlnPMDae1'))

index_name = "price_csv"

index_body = {
    "settings": {
        "index": { "number_of_shards": 1,  "number_of_replicas": 1 }
    },
    "mappings": {
        "properties": {
            "Stock_id" : {"type" : "keyword"},
            "Date" : {"type" : "date", "format" : "yyyy-MM-dd"},
            "Open" : {"type" : "float"},
            "High" : {"type" : "float"},
            "Low" : {"type" : "float"},
            "Close" : {"type" : "float"},
            "Adj Close" : {"type" : "float"},
            "Volume" : {"type" : "integer"}
        }
  }
}

result = es.indices.create(index=index_name, body=index_body)