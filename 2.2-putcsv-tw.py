import csv

from elasticsearch import Elasticsearch, helpers

es = Elasticsearch("https://464d6c48623b49a18b4d04d8dd301996.asia-east1.gcp.elastic-cloud.com:9243", http_auth=('elastic', 'q4dT1Cht7K8aSGOIlnPMDae1'))

stock_list = [100]

index_name = "price_tw"

for stock_id in stock_list:
    stock_id_price_path = "{0}/{1}.csv".format("/Users/terrychiu/Documents/work/elastic_scearch2", stock_id)
    print(stock_id_price_path)
    try:
        with open(stock_id_price_path, newline='') as csvfile:
            rows = csv.reader(csvfile)
            documents = []
            for row in rows:
                document = {}
                document['證券代號']    = row[0]
                document['證券名稱']   = row[1]
                document['Date']        = row[2]
                document['成交股數' ]   = row[3]
                document['成交金額']   = row[4]
                document['開盤價']        = row[5]
                document['最高價' ]        = row[6]
                document['最低價']         = row[7]
                document['收盤價']       = row[8]
                document['漲跌價差']   = row[9]
                document['成交筆數']      = row[10]

                action = {}
                actionProperties = {}
                actionProperties["_id"] = document['證券代號'] + document['Date']
                action["index"] = actionProperties
                documents.append(action)
                documents.append(document)
                
                result = es.bulk(body=documents, index=index_name)
                
    except IOError:
        print("File not accessible")