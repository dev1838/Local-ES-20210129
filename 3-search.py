from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

es = Elasticsearch("https://464d6c48623b49a18b4d04d8dd301996.asia-east1.gcp.elastic-cloud.com:9243", http_auth=('elastic', 'q4dT1Cht7K8aSGOIlnPMDae1'))

this_index = 'price_csv'

searchBody = {
    "sort" : [
        { "Date" : {"order" : "desc"}}
    ],
    "query" : {
        "bool": {
            "must": {
                "match": { 
                    "Stock_id": 700 
                }
            },
            "filter": {
                "range": {
                    "Date": {
                        "gte": "now-10000d/d",
                        "lt": "now/d"
                    }
                }
            }
        }        
    }
}

response = es.search(index = this_index,body=searchBody)

print(response["hits"]["total"]["value"])  
print(response["hits"]["hits"][0]["_source"]) 

#Elasticsearch DSL
s = Search(using=es, index = this_index) \
    .filter("range", Date={"gte": "now-10000d/d","lt": "now/d"}) \
    .query("match", Stock_id=700) \
    .sort({"Date": {"order": "desc"}})    

response = s.execute()

print(response["hits"]["total"]["value"]) 
print(response["hits"]["hits"][0]["_source"]) 

print(s.to_dict())
