from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

es = Elasticsearch("https://464d6c48623b49a18b4d04d8dd301996.asia-east1.gcp.elastic-cloud.com:9243", http_auth=('elastic', 'q4dT1Cht7K8aSGOIlnPMDae1'))

s = Search(using=es, index="price_csv") \
    .query("match", Date="2020-01-27") \
    .filter( "script", script={
                        "lang": "painless",
                        "source": "doc['Close'].value >= params.low_price && doc['Close'].value <= params.high_price",
                        "params": {
                            "low_price":  10,
                            "high_price": 1000
                        }
                       }
    )
response = s.execute()

print(response['hits']['total']['value'])