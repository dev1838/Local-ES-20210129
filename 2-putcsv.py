import csv

from elasticsearch import Elasticsearch, helpers

es = Elasticsearch("https://464d6c48623b49a18b4d04d8dd301996.asia-east1.gcp.elastic-cloud.com:9243", http_auth=('elastic', 'q4dT1Cht7K8aSGOIlnPMDae1'))

stock_list = [500, 700]

index_name = "price_csv"

for stock_id in stock_list:
    stock_id_price_path = "{0}/{1}.csv".format("/Users/terrychiu/Documents/work/electic_scearch2", stock_id)
    print(stock_id_price_path)
    try:
        with open(stock_id_price_path, newline='') as csvfile:
            rows = csv.reader(csvfile)
            documents = []
            for row in rows:
                document = {}
                document['Stock_id']    = row[0]
                document['Date']        = row[1]
                document['Open']        = row[2]
                document['High']        = row[3]
                document['Low']         = row[4]
                document['Close']       = row[5]
                document['Adj Close']   = row[6]
                document['Volume']      = row[7]
    
                action = {}
                actionProperties = {}
                actionProperties["_id"] = document['Stock_id'] + document['Date']
                action["index"] = actionProperties
                documents.append(action)
                documents.append(document)
                
                result = es.bulk(body=documents, index=index_name)
                
    except IOError:
        print("File not accessible")