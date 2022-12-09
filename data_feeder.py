from datetime import datetime
from elasticsearch import Elasticsearch, helpers
import time
import json
es = Elasticsearch('https://localhost:9200',ca_certs="/home/tejender/elasticsearch-8.5.2/config/certs/http_ca.crt",basic_auth=('elastic','W68wQJhDyzEy1xQQBR=1'))


def documents():
    actions={}
    with open('new.json') as file:
        data_dict=json.load(file)

        for attr in data_dict:
            for place in data_dict[attr]:
                try:
                    doc = {
                            "name" : place,
                            "latitude":data_dict[attr][place]['latitude'],
                            "longitude":data_dict[attr][place]['longitude']
                        }
                    action = {"_index" : "jaipur_index",
                            "_source" : doc}
                    yield action
                except:
                    continue
        
        return actions

if __name__ == "__main__":
    actions = documents()
    helpers.bulk(es, actions, chunk_size=500)
    es.indices.refresh(index='jaipur_index')

