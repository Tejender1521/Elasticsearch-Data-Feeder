from datetime import datetime
from elasticsearch import Elasticsearch, helpers
es = Elasticsearch('https://localhost:9200',ca_certs="/home/tejender/elasticsearch-8.5.2/config/certs/http_ca.crt",basic_auth=('elastic','W68wQJhDyzEy1xQQBR=1'))

resp = es.search(index="jaipur_index", query={"match": {"name": "Bhagwandas"}})
print("Got %d Hits:" % resp['hits']['total']['value'])
for hit in resp['hits']['hits']:
    print("%(name)s %(latitude)s: %(longitude)s" % hit["_source"])